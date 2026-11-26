import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import express, { Request, Response } from 'express';
import { createServer as createViteServer } from 'vite';
import dotenv from 'dotenv';

// Runtime safeguard for whatsapp-rust-bridge package exports
try {
  const rustBridgePkg = 'node_modules/whatsapp-rust-bridge/package.json';
  if (fs.existsSync(rustBridgePkg)) {
    const raw = fs.readFileSync(rustBridgePkg, 'utf8');
    if (!raw.includes('"main"')) {
      const parsed = JSON.parse(raw);
      parsed.main = './dist/index.js';
      if (parsed.exports && parsed.exports['.']) {
        parsed.exports['.'].default = './dist/index.js';
        parsed.exports['.'].require = './dist/index.js';
      }
      fs.writeFileSync(rustBridgePkg, JSON.stringify(parsed, null, 2));
    }
  }
} catch (e) {}

import { botEngine, getBotEngine, autoBootAllRegisteredBots } from './botEngine';
import { askGemini } from './geminiService';
import { getPluginStatusList } from './pluginService';
import { adminStore } from './adminStore';
import { ensureYtDlpBinary, cleanStaleTempFiles } from './audioService';
import {
  saveAudioFile,
  deleteAudioFile,
  getAudioFileStream,
  validateAudioBuffer,
  formatBytes,
} from './voiceService';
import { normalizeWhatsAppNumber } from '../utils/phone';
import { gatewayService } from './gatewayService';
import {
  getBusinessStore,
  updateBusinessStoreSettings,
  addBusinessProduct,
  updateBusinessProduct,
  deleteBusinessProduct,
  saveBusinessAssetFromBase64,
} from './businessStore';
import {
  getBusinessPayState,
  updateBusinessPayConfig,
  rotateBusinessPaySecret,
  resolveTenantByPaySecret,
  updateBusinessPayOrderStatus,
  touchPayDevice,
  saveBusinessPayState,
  ingestPayWebhookEvent,
  getApkBinaryPath,
  getApkBinaryInfo,
  setBusinessPayOrigin,
  buildBotLinkKey,
} from './businessPayService';
import {
  getAutoOrderDashboardSummary,
  updateAutoOrderSettings,
  updateGuildFlowSettings,
  upsertProductMapping,
  deleteProductMapping,
  createAutoOrder,
  createGuildServiceOrder,
  processGatewayPaymentForAutoOrders,
  executeAutoOrderDelivery,
  adminApproveOrderPayment,
  adminRejectOrCancelOrder,
  refreshFFGloryBalance,
  refreshFFGloryPricing,
  ensureFreshFFGloryData,
  resolveFFGloryCredentials,
  getAutoOrderState,
  saveAutoOrderState,
  appendApiError,
  findOrderByPortalToken,
  submitGuildConfiguration,
  fetchOrderLiveGlory,
  sendPhase5GuildConfigCard,
  buildOrderPortalUrl,
  resolveStoredReceiptFilePath,
  registerPublicPortalBaseUrl,
  type GuildBotQuantity,
} from './autoOrderService';
import { ffgloryClient, FFGloryApiError } from './ffgloryClient';
import {
  AuthenticatedPrincipal,
  isAuthorizedAdminEmail,
  issueSessionToken,
  requireAuthenticatedUser,
  requireAuthorizedAdmin,
  verifyFirebaseIdToken,
} from './authService';
import { subscriptionService, type SubscriptionPlanId } from './subscriptionService';

dotenv.config();

const rootDir = process.cwd();

async function startServer() {
  const app = express();
  const PORT = process.env.SERVER_PORT || process.env.PORT || 3000;

  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));
  // CORS configuration to support custom domains & Cloudflare Workers (connect.yashi-halls.workers.dev)
  app.use((req: Request, res: Response, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, x-user-id, x-yashi-pay-secret');
    if (req.method === 'OPTIONS') {
      return res.sendStatus(200);
    }
    try {
      const fwdProto = String(req.headers['x-forwarded-proto'] || req.protocol || 'https').split(',')[0].trim();
      const fwdHost = String(req.headers['x-forwarded-host'] || req.get('host') || '').split(',')[0].trim();
      if (fwdHost) {
        registerPublicPortalBaseUrl(`${fwdProto || 'https'}://${fwdHost}`);
      }
    } catch {}
    next();
  });

  const TIER_RANK: Record<SubscriptionPlanId, number> = {
    mini: 0,
    pro: 1,
    business: 2,
  };

  const resolveScopedUserId = (req: Request, defaultFallback = 'primary'): string => {
    const rawId = req.headers['x-user-id'] || req.query.userId || req.body?.userId;
    const requested =
      typeof rawId === 'string' && rawId.trim() && rawId !== 'null' && rawId !== 'undefined'
        ? rawId.trim()
        : '';
    const principal = (req as any).authPrincipal as AuthenticatedPrincipal | undefined;

    if (!principal) {
      return requested || defaultFallback;
    }

    if (principal.isAdmin) {
      return requested || principal.uid || defaultFallback;
    }

    const emailKey = `user_${principal.email.toLowerCase().replace(/[^a-zA-Z0-9_-]/g, '_')}`;
    const allowedIds = new Set([
      principal.uid,
      `${principal.uid}__business`,
      emailKey,
      `${emailKey}__business`,
    ]);
    if (requested && allowedIds.has(requested)) {
      return requested;
    }
    return defaultFallback.endsWith('__business') ? `${principal.uid}__business` : principal.uid;
  };

  // Multi-Tenant Isolation Helper: Resolves customer's private BotEngine based on authenticated UID
  const getEngineForReq = (req: Request) => {
    return getBotEngine(resolveScopedUserId(req, 'primary'));
  };

  const getEffectiveCallerTier = (req: Request): SubscriptionPlanId => {
    const principal = (req as any).authPrincipal as AuthenticatedPrincipal | undefined;
    if (!principal) return 'mini';
    return subscriptionService.getEffectiveTier(principal.uid, principal.email, principal.isAdmin);
  };

  const requireBusinessTier = (req: Request, res: Response, next: () => void) => {
    const principal = (req as any).authPrincipal as AuthenticatedPrincipal | undefined;
    if (!principal) {
      return res.status(401).json({ success: false, message: 'Authentication required.' });
    }
    const tier = subscriptionService.getEffectiveTier(principal.uid, principal.email, principal.isAdmin);
    if (tier !== 'business' && !principal.isAdmin) {
      return res.status(403).json({
        success: false,
        code: 'BUSINESS_TIER_REQUIRED',
        currentPlan: tier,
        message: 'Active BUSINESS subscription required. Submit manual bank transfer and wait for admin approval.',
      });
    }
    next();
  };

  // --- AUTHENTICATION SESSION EXCHANGE ---
  app.post('/api/auth/session', async (req: Request, res: Response) => {
    try {
      const { idToken, email, username, uid } = req.body || {};

      if (typeof idToken === 'string' && idToken.trim()) {
        const verified = await verifyFirebaseIdToken(idToken.trim());
        if (verified) {
          adminStore.syncCustomer(verified.uid, verified.email, verified.username);
          const sessionToken = issueSessionToken({
            uid: verified.uid,
            email: verified.email,
            username: verified.username,
            firebaseVerified: verified.firebaseVerified,
          });
          return res.json({
            success: true,
            token: sessionToken,
            principal: verified,
            subscription: subscriptionService.getUserSubscriptionView(
              verified.uid,
              verified.email,
              verified.username
            ).subscription,
          });
        }
        // If Firebase cert fetch fails transiently in isolated network, allow fallback only if email provided
      }

      const cleanEmail = String(email || '').trim().toLowerCase();
      if (!cleanEmail || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)) {
        return res.status(400).json({
          success: false,
          message: 'Valid authenticated email or Firebase ID token is required.',
        });
      }

      const safeUid =
        typeof uid === 'string' && uid.trim() && /^[a-zA-Z0-9_-]{4,128}$/.test(uid.trim())
          ? uid.trim()
          : `user_${cleanEmail.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
      const cleanName = String(username || cleanEmail.split('@')[0] || 'User').trim().slice(0, 64);

      adminStore.syncCustomer(safeUid, cleanEmail, cleanName);

      const sessionToken = issueSessionToken({
        uid: safeUid,
        email: cleanEmail,
        username: cleanName,
        firebaseVerified: false,
      });

      const view = subscriptionService.getUserSubscriptionView(safeUid, cleanEmail, cleanName);
      return res.json({
        success: true,
        token: sessionToken,
        principal: {
          uid: safeUid,
          email: cleanEmail,
          username: cleanName,
          isAdmin: isAuthorizedAdminEmail(cleanEmail),
          firebaseVerified: false,
          provider: 'session',
        },
        subscription: view.subscription,
      });
    } catch (err: any) {
      return res.status(500).json({
        success: false,
        message: err?.message || 'Failed to establish session token',
      });
    }
  });

  // --- SUBSCRIPTION & MANUAL BANK TRANSFER PAYMENT WORKFLOW ---
  app.get('/api/subscriptions/config', (_req: Request, res: Response) => {
    try {
      res.json({
        success: true,
        data: subscriptionService.getPublicBillingConfig(),
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load billing config' });
    }
  });

  app.get('/api/subscriptions/me', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const view = subscriptionService.getUserSubscriptionView(
        principal.uid,
        principal.email,
        principal.username
      );
      res.json({
        success: true,
        data: {
          ...view,
          isAdmin: principal.isAdmin,
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load subscription status' });
    }
  });

  app.post('/api/subscriptions/notifications/:id/read', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const ok = subscriptionService.markNotificationRead(principal.uid, principal.email, req.params.id);
      res.json({ success: ok });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update notification' });
    }
  });

  app.post('/api/subscriptions/requests', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const { packageId, transactionReference, paymentDate, receiptBase64, notes } = req.body || {};
      const result = subscriptionService.submitManualPayment({
        userId: principal.uid,
        email: principal.email,
        username: principal.username,
        packageId,
        transactionReference,
        paymentDate,
        receiptBase64,
        notes,
      });
      return res.status(result.statusCode).json({
        success: result.success,
        message: result.message,
        request: result.request || null,
      });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to submit manual payment request' });
    }
  });

  app.get('/api/subscriptions/receipts/:fileName', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const resolved = subscriptionService.resolveReceiptFileForRequest(req.params.fileName, {
        uid: principal.uid,
        email: principal.email,
        isAdmin: principal.isAdmin,
      });
      if (!resolved.allowed || !resolved.filePath) {
        return res.status(403).json({
          success: false,
          message: 'Forbidden or receipt file not found.',
        });
      }
      res.setHeader('Content-Type', resolved.mimeType || 'image/png');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Cache-Control', 'private, max-age=300');
      return res.sendFile(resolved.filePath);
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to load receipt image' });
    }
  });

  app.get('/api/subscriptions/admin/overview', requireAuthorizedAdmin, (_req: Request, res: Response) => {
    try {
      const overview = subscriptionService.getAdminOverview();
      res.json({ success: true, data: overview });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load subscription admin overview' });
    }
  });

  app.post('/api/subscriptions/admin/requests/:id/review', requireAuthorizedAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = String((req as any).adminEmail || '');
      const actionRaw = String(req.body?.action || '').trim().toUpperCase();
      if (actionRaw !== 'APPROVE' && actionRaw !== 'REJECT') {
        return res.status(400).json({ success: false, message: 'Action must be APPROVE or REJECT.' });
      }
      const result = subscriptionService.reviewPaymentRequest({
        requestId: req.params.id,
        action: actionRaw as 'APPROVE' | 'REJECT',
        adminEmail,
        rejectionReason: req.body?.rejectionReason,
      });
      return res.status(result.statusCode).json({
        success: result.success,
        message: result.message,
        request: result.request || null,
        subscription: result.subscription || null,
      });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to review payment request' });
    }
  });

  app.post('/api/subscriptions/admin/config', requireAuthorizedAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = String((req as any).adminEmail || '');
      const { bankDetails, packagePricesLkr } = req.body || {};
      const updated = subscriptionService.adminUpdateConfig({ bankDetails, packagePricesLkr }, adminEmail);
      res.json({
        success: true,
        data: updated,
        message: 'Bank account details and monthly package prices updated successfully.',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update billing config' });
    }
  });

  app.post('/api/subscriptions/admin/downgrade', requireAuthorizedAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = String((req as any).adminEmail || '');
      const target = String(req.body?.userIdOrEmail || '').trim();
      if (!target) {
        return res.status(400).json({ success: false, message: 'userIdOrEmail is required.' });
      }
      const updated = subscriptionService.adminForceExpireOrDowngrade(target, adminEmail);
      if (!updated) {
        return res.status(404).json({ success: false, message: 'Subscription record not found.' });
      }
      res.json({ success: true, data: updated, message: `Downgraded ${updated.email} to MINI.` });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to downgrade user' });
    }
  });

  // Health check endpoint
  // 24/7 Cloud Keep-Alive Endpoint for UptimeRobot / Cron / Cloudflare Workers
  app.get('/api/keepalive', (req: Request, res: Response) => {
    const primaryEngine = getBotEngine('primary');
    res.json({
      status: 'active',
      keepAlivePulse: true,
      timestamp: new Date().toISOString(),
      uptimeSeconds: Math.floor(process.uptime()),
      botState: primaryEngine.state.connectionStatus,
      isOnline: primaryEngine.state.isOnline,
      message: '24/7 Always-On heartbeat acknowledged. Cloud Run container is kept awake!'
    });
  });

  // --- ANDROID BANK NOTIFICATION GATEWAY ENDPOINTS ---
  app.get('/api/gateway/status', (req: Request, res: Response) => {
    try {
      const status = gatewayService.getStatus();
      res.json({ success: true, data: status });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get gateway status' });
    }
  });

  // Webhook endpoint called by Android APK
  app.post('/api/gateway/bank-notification', (req: Request, res: Response) => {
    try {
      const { token, deviceId, deviceName, title, message, sender, rawText, batteryLevel } = req.body;
      const result = gatewayService.registerNotification({
        token,
        deviceId: deviceId || 'android-device',
        deviceName: deviceName || 'Android Phone',
        title,
        message,
        sender,
        rawText,
        batteryLevel,
      });

      if (result.matchedSlip) {
        botEngine.addLog(
          'INFO',
          `🎉 [PAYMENT AUTO-VERIFIED] Bank SMS matched customer ${result.matchedSlip.userName}! Amount: LKR ${result.matchedSlip.amount}, Ref: ${result.matchedSlip.ref}`
        );
      } else {
        botEngine.addLog(
          'INFO',
          `📥 [BANK ALERT SYNCED] ${result.notification.bank}: LKR ${result.notification.amount} (Ref: ${result.notification.ref})`
        );
      }

      res.json({ success: true, notification: result.notification, matched: Boolean(result.matchedSlip) });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to process bank alert' });
    }
  });

  // Test simulation endpoint for testing the matching pipeline directly from Dashboard
  app.post('/api/gateway/test-simulate', (req: Request, res: Response) => {
    try {
      const { amount, ref, bank } = req.body;
      const result = gatewayService.simulateTestNotification(Number(amount) || 1500, ref, bank);
      botEngine.addLog('INFO', `🧪 [GATEWAY TEST] Simulated incoming ${result.notification.bank} SMS: LKR ${result.notification.amount} (Ref: ${result.notification.ref})`);
      res.json({ success: true, data: result });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Simulation failed' });
    }
  });

  // Rules management
  app.get('/api/gateway/rules', (req: Request, res: Response) => {
    res.json({ success: true, data: gatewayService.getRules() });
  });

  app.post('/api/gateway/rules', (req: Request, res: Response) => {
    try {
      const { name, sender, keyword, ignoreKeyword } = req.body;
      const newRule = gatewayService.addRule({ name, sender, keyword, ignoreKeyword, active: true });
      res.json({ success: true, data: newRule });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to add rule' });
    }
  });

  app.post('/api/gateway/rules/:id/toggle', (req: Request, res: Response) => {
    const updated = gatewayService.toggleRule(req.params.id);
    if (updated) return res.json({ success: true, data: updated });
    res.status(404).json({ success: false, message: 'Rule not found' });
  });

  app.delete('/api/gateway/rules/:id', (req: Request, res: Response) => {
    const success = gatewayService.deleteRule(req.params.id);
    res.json({ success });
  });

  // Bot Token and Gateway Configuration
  app.post('/api/gateway/config', (req: Request, res: Response) => {
    try {
      const { botToken, pairedBotNumber } = req.body;
      const config = gatewayService.setConfig(botToken, pairedBotNumber);
      res.json({ success: true, data: config });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update config' });
    }
  });

  app.get('/api/health', (req: Request, res: Response) => {
    const memory = process.memoryUsage();
    res.json({
      status: 'ok',
      service: 'YASHI HALLS — WhatsApp Bot Management Engine (MINI • PRO • BUSINESS)',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      nodeVersion: process.version,
      memory: {
        rssMb: Math.round(memory.rss / 1024 / 1024),
        heapUsedMb: Math.round(memory.heapUsed / 1024 / 1024),
        heapTotalMb: Math.round(memory.heapTotal / 1024 / 1024),
      },
      botState: botEngine.getState().connectionStatus,
    });
  });

  // Get live Bot State and metrics (isolated per customer, server-enforced subscription tier)
  app.get('/api/bot/status', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const effectiveTier = getEffectiveCallerTier(req);
      const currentPlan = (engine.state?.config?.plan || 'mini') as SubscriptionPlanId;
      if (TIER_RANK[currentPlan] > TIER_RANK[effectiveTier]) {
        engine.setPlan(effectiveTier);
      }
      const state = engine.getState();
      res.json({
        success: true,
        data: state,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err?.message || 'Failed to fetch bot state' });
    }
  });

  // Get live runtime logs (isolated per customer)
  app.get('/api/bot/logs', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const logs = engine.getLogs();
      res.json({
        success: true,
        data: logs,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err?.message || 'Failed to fetch bot logs' });
    }
  });

  // Start Bot instance (isolated per customer)
  app.post('/api/bot/start', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const { phoneNumber } = req.body || {};
      const cleanPhone = phoneNumber ? normalizeWhatsAppNumber(phoneNumber) : undefined;
      const engine = getEngineForReq(req);
      const effectiveTier = getEffectiveCallerTier(req);
      if (TIER_RANK[(engine.state?.config?.plan || 'mini') as SubscriptionPlanId] > TIER_RANK[effectiveTier]) {
        engine.setPlan(effectiveTier);
      }
      const result = await engine.start(cleanPhone);
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to start bot' });
    }
  });

  // Stop Bot instance (isolated per customer)
  app.post('/api/bot/stop', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const result = await engine.stop();
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to stop bot' });
    }
  });

  // Restart Bot instance (isolated per customer)
  app.post('/api/bot/restart', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const result = await engine.restart();
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to restart bot' });
    }
  });

  // Request WhatsApp Pairing Code for phone number (isolated per customer)
  app.post(['/api/bot/pair/code', '/api/bot/pair'], requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const { phoneNumber } = req.body;
      if (!phoneNumber) {
        return res.status(400).json({ success: false, message: 'Phone number is required.' });
      }

      const cleanPhone = normalizeWhatsAppNumber(phoneNumber);
      if (!cleanPhone || cleanPhone.length < 8) {
        return res.status(400).json({
          success: false,
          message: 'Invalid phone number format. Enter e.g. 0740712934 or 94740712934.',
        });
      }

      const engine = getEngineForReq(req);
      const result = await engine.requestPairingCode(cleanPhone);
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to request pairing code' });
    }
  });

  // Request QR Code pairing mode (isolated per customer)
  app.post('/api/bot/pair/qr', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const { forceNew } = req.body || {};
      const engine = getEngineForReq(req);
      const result = await engine.generateQrCode(Boolean(forceNew));
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to generate QR Code' });
    }
  });

  // Clear Session & Credentials for clean re-pairing (isolated per customer)
  app.post('/api/bot/clear-session', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const result = await engine.clearSession();
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to clear session' });
    }
  });

  // Set Bot Plan — STRICTLY ENFORCED against server-side admin-approved subscription!
  app.post('/api/bot/plan', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const { plan } = req.body || {};
      if (plan !== 'mini' && plan !== 'pro' && plan !== 'business') {
        return res.status(400).json({ success: false, message: 'Invalid plan. Allowed: "mini", "pro", or "business".' });
      }

      const effectiveTier = getEffectiveCallerTier(req);
      if (TIER_RANK[plan as SubscriptionPlanId] > TIER_RANK[effectiveTier]) {
        return res.status(403).json({
          success: false,
          code: 'SUBSCRIPTION_REQUIRED',
          currentPlan: effectiveTier,
          requestedPlan: plan,
          message: `${plan.toUpperCase()} subscription is not active on your account. Submit a manual bank transfer payment and wait for admin approval.`,
        });
      }

      const engine = getEngineForReq(req);
      const updatedState = engine.setPlan(plan);
      res.json({ success: true, plan, data: updatedState });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update plan' });
    }
  });

  // Paid packages cannot be self-activated with client coin clicks anymore
  app.post('/api/bot/purchase-plan', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const { plan } = req.body || {};
      if (plan !== 'mini' && plan !== 'pro' && plan !== 'business') {
        return res.status(400).json({ success: false, message: 'Invalid plan. Allowed: "mini", "pro", or "business".' });
      }

      if (plan !== 'mini' && !principal.isAdmin) {
        return res.status(403).json({
          success: false,
          code: 'MANUAL_BANK_TRANSFER_REQUIRED',
          message: 'Paid packages (PRO / BUSINESS) require a manual bank transfer receipt verified and approved by an admin.',
        });
      }

      const engine = getEngineForReq(req);
      const updatedState = engine.setPlan(plan);
      res.json({ success: true, plan, data: updatedState });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to process plan update' });
    }
  });

  // Sync WhatsApp Bot Profile Picture to Brand Logo (isolated per customer)
  app.post('/api/bot/sync-profile-pic', requireAuthenticatedUser, async (req: Request, res: Response) => {
    try {
      const { logoUrl } = req.body || {};
      const engine = getEngineForReq(req);
      const result = await engine.syncProfilePictureWithLogo(logoUrl);
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to sync profile picture' });
    }
  });

  // Reset Quotas (daily refresh - isolated per customer)
  app.post('/api/bot/reset-quotas', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const quotas = engine.resetQuotas();
      res.json({ success: true, quotas });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to reset quotas' });
    }
  });

  // Get Bot Configuration (isolated per customer)
  app.get('/api/bot/config', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const config = engine.getState().config;
      res.json({ success: true, data: config });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to read config' });
    }
  });

  // Update Bot Configuration (isolated per customer; strips client-supplied plan & gates paid flags)
  app.post('/api/bot/config', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const engine = getEngineForReq(req);
      const effectiveTier = getEffectiveCallerTier(req);
      const { userId: _uid, plan: _ignoredPlan, ...configUpdates } = req.body || {};

      if (effectiveTier === 'mini') {
        configUpdates.antiLink = false;
        configUpdates.antiSpam = false;
      }

      const updated = engine.updateConfig(configUpdates);
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update config' });
    }
  });

  // --- BUSINESS BOT STORE & PRODUCT CATALOG ENDPOINTS ---
  const getBizStoreTenantId = (req: Request) => {
    return resolveScopedUserId(req, 'primary__business');
  };

  app.use('/api/business-store', (req: Request, res: Response, next) => {
    if (req.method === 'GET' && req.path.startsWith('/assets/')) {
      return next();
    }
    return requireAuthenticatedUser(req, res, () => requireBusinessTier(req, res, next));
  });

  app.get('/api/business-store', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const data = getBusinessStore(tenantId);
      res.json({ success: true, data });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load business store' });
    }
  });

  app.post('/api/business-store/settings', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const {
        storeName,
        mainLogoUrl,
        mainLogoBase64,
        welcomeText,
        panelButtonText,
        gloryButtonText,
        orderInstructions,
        categories,
      } = req.body || {};

      let resolvedLogoUrl = mainLogoUrl;
      if (typeof mainLogoBase64 === 'string' && mainLogoBase64.startsWith('data:image/')) {
        resolvedLogoUrl = saveBusinessAssetFromBase64(tenantId, mainLogoBase64, 'main_logo');
      }

      const data = updateBusinessStoreSettings(tenantId, {
        storeName,
        mainLogoUrl: resolvedLogoUrl,
        welcomeText,
        panelButtonText,
        gloryButtonText,
        orderInstructions,
        categories,
      });
      res.json({ success: true, data });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update store settings' });
    }
  });

  app.post('/api/business-store/products', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const {
        name,
        category,
        platform,
        price,
        command,
        packages,
        subtitle,
        description,
        logoUrl,
        logoBase64,
        inStock,
        verified,
      } = req.body || {};

      if (!name || typeof name !== 'string' || !name.trim()) {
        return res.status(400).json({ success: false, message: 'Product name is required.' });
      }

      let resolvedLogoUrl = typeof logoUrl === 'string' ? logoUrl : '';
      if (typeof logoBase64 === 'string' && logoBase64.startsWith('data:image/')) {
        resolvedLogoUrl = saveBusinessAssetFromBase64(tenantId, logoBase64, 'product');
      }

      const data = addBusinessProduct(tenantId, {
        name: name.trim(),
        category: category || 'OTHER',
        platform: platform || 'android',
        price: price || '$0.00',
        command: typeof command === 'string' ? command : '',
        packages: Array.isArray(packages) ? packages : [],
        subtitle: subtitle || '',
        description: description || '',
        logoUrl: resolvedLogoUrl,
        inStock: inStock !== false,
        verified: verified !== false,
      });
      res.json({ success: true, data });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to add product' });
    }
  });

  app.put('/api/business-store/products/:id', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const productId = req.params.id;
      const { logoBase64, userId, ...rest } = req.body || {};

      if (typeof logoBase64 === 'string' && logoBase64.startsWith('data:image/')) {
        rest.logoUrl = saveBusinessAssetFromBase64(tenantId, logoBase64, 'product');
      }

      const data = updateBusinessProduct(tenantId, productId, rest);
      res.json({ success: true, data });
    } catch (err: any) {
      res.status(404).json({ success: false, message: err?.message || 'Failed to update product' });
    }
  });

  app.delete('/api/business-store/products/:id', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const productId = req.params.id;
      const data = deleteBusinessProduct(tenantId, productId);
      res.json({ success: true, data });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to delete product' });
    }
  });

  app.post('/api/business-store/upload-logo', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const { imageBase64, prefix } = req.body || {};
      if (!imageBase64 || typeof imageBase64 !== 'string') {
        return res.status(400).json({ success: false, message: 'imageBase64 is required' });
      }
      const assetUrl = saveBusinessAssetFromBase64(tenantId, imageBase64, prefix || 'logo');
      res.json({ success: true, url: assetUrl });
    } catch (err: any) {
      res.status(400).json({ success: false, message: err?.message || 'Failed to upload logo' });
    }
  });

  app.get('/api/business-store/assets/:tenant/:filename', (req: Request, res: Response) => {
    try {
      const safeTenant = String(req.params.tenant || '').replace(/[^a-zA-Z0-9_-]/g, '_');
      const safeFile = path.basename(String(req.params.filename || ''));
      const filePath = path.resolve(process.cwd(), 'data', 'business_assets', safeTenant, safeFile);
      if (!fs.existsSync(filePath)) {
        return res.status(404).send('Asset not found');
      }
      res.sendFile(filePath);
    } catch {
      res.status(404).send('Asset not found');
    }
  });

  // --- YASHI HALLS PAY (BUSINESS BOT APK <-> BOT API SYSTEM) ---
  const extractPaySecret = (req: Request): string => {
    const authHeader = String(req.headers['authorization'] || '');
    if (authHeader.toLowerCase().startsWith('bearer ')) {
      return authHeader.slice(7).trim();
    }
    const headerSecret = req.headers['x-yashi-pay-secret'];
    if (typeof headerSecret === 'string' && headerSecret.trim()) {
      return headerSecret.trim();
    }
    const bodySecret = req.body?.secret || req.body?.token;
    return typeof bodySecret === 'string' ? bodySecret.trim() : '';
  };

  app.get('/api/business/pay/apk/info', (req: Request, res: Response) => {
    try {
      res.json({ success: true, data: getApkBinaryInfo() });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to read APK info' });
    }
  });

  app.get('/api/business/pay/apk/download', (req: Request, res: Response) => {
    try {
      const apkPath = getApkBinaryPath();
      if (!fs.existsSync(apkPath)) {
        return res.status(404).json({ success: false, message: 'YASHI HALLS Pay APK binary not built yet.' });
      }
      res.setHeader('Content-Type', 'application/vnd.android.package-archive');
      res.setHeader('Content-Disposition', 'attachment; filename="YashiHallsPay.apk"');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Cache-Control', 'no-cache');
      return res.sendFile(apkPath);
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to download APK' });
    }
  });

  const extractPublicOrigin = (req: Request): string => {
    const protoHeader = String(req.headers['x-forwarded-proto'] || req.protocol || 'https')
      .split(',')[0]
      .trim();
    const hostHeader = String(req.headers['x-forwarded-host'] || req.get('host') || '')
      .split(',')[0]
      .trim();
    if (!hostHeader) return '';
    return `${protoHeader || 'https'}://${hostHeader}`;
  };

  app.post('/api/business/pay/apk/send-whatsapp', requireAuthenticatedUser, requireBusinessTier, async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const reqOrigin = extractPublicOrigin(req);
      if (reqOrigin) setBusinessPayOrigin(tenantId, reqOrigin);

      const bizEngine = getBotEngine(tenantId);
      const isBizOnline = Boolean(bizEngine && bizEngine.sock && bizEngine.state.isOnline);

      if (!isBizOnline || !bizEngine) {
        return res.status(503).json({
          success: false,
          message: 'Business Bot දැනට Online නැත. පළමුව Business Bot එක Connect කර නැවත උත්සාහ කරන්න (හෝ Business Bot chat එකේ .payapk යවන්න).',
        });
      }

      const rawPhone =
        (typeof req.body?.phoneNumber === 'string' && req.body.phoneNumber.trim()) ||
        bizEngine.state?.config?.ownerNumber ||
        bizEngine.state?.pairedNumber ||
        '';

      const cleanDigits = normalizeWhatsAppNumber(rawPhone) || String(rawPhone).replace(/\D/g, '');
      if (!cleanDigits || cleanDigits.length < 8) {
        return res.status(400).json({
          success: false,
          message: 'වලංගු WhatsApp අංකයක් ඇතුළත් කරන්න (උදා: 0740712934).',
        });
      }

      const targetJid = `${cleanDigits}@s.whatsapp.net`;
      const result = await bizEngine.sendYashiPayApkToWhatsApp(targetJid);
      if (!result.ok) {
        return res.status(500).json({
          success: false,
          message: result.error || 'Failed to send APK to Business Bot WhatsApp',
        });
      }

      return res.json({
        success: true,
        sentTo: cleanDigits,
        viaBot: 'Business Bot',
        message: `YashiHallsPay-v2.0.0.apk ගොනුව +${cleanDigits} WhatsApp වෙත Business Bot හරහා සාර්ථකව යවන ලදී!`,
      });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to deliver APK via Business Bot' });
    }
  });

  app.get('/api/business/pay/status', requireAuthenticatedUser, requireBusinessTier, (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const reqOrigin = extractPublicOrigin(req);
      if (reqOrigin) setBusinessPayOrigin(tenantId, reqOrigin);
      const state = getBusinessPayState(tenantId);
      const apkInfo = getApkBinaryInfo();
      const linkKey = buildBotLinkKey(tenantId, reqOrigin);
      const totalPaidLkr = state.events
        .filter((ev) => ev.status !== 'ignored')
        .reduce((acc, ev) => acc + (Number(ev.amountLkr) || 0), 0);

      res.json({
        success: true,
        data: {
          ...state,
          linkKey,
          apk: apkInfo,
          stats: {
            totalPaidLkr,
            matchedCount: state.events.filter((ev) => ev.status === 'matched').length,
            pendingOrdersCount: state.orders.filter((o) => o.status === 'pending_payment').length,
            activeDevicesCount: state.devices.filter((d) => Date.now() - new Date(d.lastSeenAt).getTime() < 1000 * 60 * 15).length,
          },
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load Business Pay state' });
    }
  });

  app.post('/api/business/pay/config', requireAuthenticatedUser, requireBusinessTier, (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const {
        enabled,
        autoMatchOrders,
        notifyOwnerOnPayment,
        notifyCustomerOnMatch,
        bankDetailsText,
        minAmountLkr,
        lkrPerUsdRate,
      } = req.body || {};

      const updated = updateBusinessPayConfig(tenantId, {
        enabled,
        autoMatchOrders,
        notifyOwnerOnPayment,
        notifyCustomerOnMatch,
        bankDetailsText,
        minAmountLkr,
        lkrPerUsdRate,
      });
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update Pay settings' });
    }
  });

  app.post('/api/business/pay/rotate-secret', requireAuthenticatedUser, requireBusinessTier, (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const updated = rotateBusinessPaySecret(tenantId);
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to rotate secret key' });
    }
  });

  app.post('/api/business/pay/orders/:id/status', requireAuthenticatedUser, requireBusinessTier, async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const status = req.body?.status;
      if (status !== 'pending_payment' && status !== 'paid' && status !== 'cancelled') {
        return res.status(400).json({ success: false, message: 'Invalid order status' });
      }
      const order = updateBusinessPayOrderStatus(tenantId, req.params.id, status);
      if (!order) {
        return res.status(404).json({ success: false, message: 'Order not found' });
      }
      if (status === 'paid') {
        const engine = getBotEngine(tenantId);
        if (engine.sock && engine.state.isOnline && order.customerJid) {
          const msgText = [
            `✅ *PAYMENT CONFIRMED BY OWNER*`,
            `🧾 *Order ID:* ${order.id}`,
            `🛍️ *Product:* ${order.productName}${order.packageDays ? ` (${order.packageDays})` : ''}`,
            `💰 *Amount:* ${order.priceDisplay}`,
            ``,
            `ඔබගේ Order එක තහවුරු වී ඇත. Thank you! 🙏`,
          ].join('\n');
          await engine.sock.sendMessage(order.customerJid, { text: msgText }).catch(() => {});
        }
      }
      res.json({ success: true, data: order });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update order status' });
    }
  });

  app.post('/api/business/pay/test', requireAuthenticatedUser, requireBusinessTier, async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const state = getBusinessPayState(tenantId);
      const amountNum = Number(req.body?.amountLkr) > 0 ? Number(req.body.amountLkr) : 1500;
      const bankName = String(req.body?.bank || 'Commercial Bank').slice(0, 48);
      const ref = String(req.body?.reference || Math.floor(10000000 + Math.random() * 90000000)).slice(0, 24);
      const rawMessage = `A/C ***412 Credited with LKR ${amountNum.toFixed(2)} via ${bankName}. Ref: ${ref}. [YASHI HALLS Pay Test]`;

      const result = ingestPayWebhookEvent(state, {
        source: 'test',
        sender: bankName,
        rawMessage,
        amountLkr: amountNum,
        reference: ref,
        deviceId: 'dashboard-simulator',
        deviceName: 'Dashboard Test Simulator',
      });

      const engine = getBotEngine(tenantId);
      await engine.handleIncomingBusinessPayment(result.event, result.matchedOrder);

      // Also feed test payment into Auto Order matching & Guild Phase 5 engine
      const autoOrderResult = await processGatewayPaymentForAutoOrders(
        tenantId,
        result.event,
        engine.getAutoOrderWhatsAppDispatcher(),
        {
          targetOrderId: typeof req.body?.orderId === 'string' ? req.body.orderId : undefined,
          verifiedBy: 'TEST_SIMULATOR',
        }
      );

      res.json({
        success: true,
        data: { ...result, autoOrderResult },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Test webhook failed' });
    }
  });

  // Mobile APK -> Business Bot Connectivity Check
  app.post('/api/business/pay/ping', (req: Request, res: Response) => {
    try {
      const secret = extractPaySecret(req);
      const preferredTenant = typeof req.body?.tenantId === 'string' ? req.body.tenantId : (typeof req.query.tenantId === 'string' ? req.query.tenantId : undefined);
      const state = resolveTenantByPaySecret(secret, preferredTenant);
      if (!state) {
        return res.status(401).json({ success: false, message: 'Invalid Secret Key. Copy the secret key from Business Bot Pay panel.' });
      }

      touchPayDevice(state, {
        deviceId: req.body?.deviceId,
        deviceName: req.body?.deviceName,
        appVersion: req.body?.appVersion,
        batteryLevel: req.body?.batteryLevel,
        simOperator: req.body?.simOperator,
      });
      saveBusinessPayState(state);

      const store = getBusinessStore(state.tenantId);
      const engine = getBotEngine(state.tenantId);

      return res.json({
        success: true,
        storeName: store.storeName,
        botName: engine.state.config.botName || 'YASHI BUSINESS',
        botOnline: Boolean(engine.state.isOnline),
        enabled: state.config.enabled,
        pendingOrdersCount: state.orders.filter((o) => o.status === 'pending_payment').length,
      });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Ping failed' });
    }
  });

  // Mobile APK -> Business Bot Payment Webhook
  app.post('/api/business/pay/webhook', async (req: Request, res: Response) => {
    try {
      const secret = extractPaySecret(req);
      const preferredTenant = typeof req.body?.tenantId === 'string' ? req.body.tenantId : (typeof req.query.tenantId === 'string' ? req.query.tenantId : undefined);
      const state = resolveTenantByPaySecret(secret, preferredTenant);
      if (!state) {
        return res.status(401).json({ success: false, message: 'Unauthorized: Invalid YASHI HALLS Pay Secret Key' });
      }

      if (!state.config.enabled) {
        return res.status(403).json({ success: false, message: 'YASHI HALLS Pay Gateway is currently paused in Business Bot settings.' });
      }

      const rawMessage = String(req.body?.rawMessage || req.body?.message || req.body?.text || '').trim();
      if (!rawMessage) {
        return res.status(400).json({ success: false, message: 'rawMessage is required' });
      }

      const sourceRaw = String(req.body?.source || 'notification').toLowerCase();
      const source: 'sms' | 'notification' = sourceRaw === 'sms' ? 'sms' : 'notification';

      const result = ingestPayWebhookEvent(state, {
        source,
        sender: req.body?.sender,
        appPackage: req.body?.appPackage,
        rawMessage,
        amountLkr: req.body?.amountLkr !== undefined ? Number(req.body.amountLkr) : undefined,
        balanceLkr: req.body?.balanceLkr !== undefined ? Number(req.body.balanceLkr) : undefined,
        reference: req.body?.reference,
        deviceId: req.body?.deviceId,
        deviceName: req.body?.deviceName,
        appVersion: req.body?.appVersion,
        batteryLevel: req.body?.batteryLevel !== undefined ? Number(req.body.batteryLevel) : undefined,
        simOperator: req.body?.simOperator,
      });

      let autoOrderOutcome: string | null = null;
      let autoOrderMatchedId: string | null = null;

      if (!result.duplicate && result.event.status !== 'ignored') {
        const engine = getBotEngine(state.tenantId);
        await engine.handleIncomingBusinessPayment(result.event, result.matchedOrder);

        // Process payment through Auto Order Matching & FFGlory Automatic Delivery
        const aoRes = await processGatewayPaymentForAutoOrders(
          state.tenantId,
          result.event,
          engine.getAutoOrderWhatsAppDispatcher(),
          { verifiedBy: 'AUTO_GATEWAY' }
        );
        autoOrderOutcome = aoRes.outcome;
        autoOrderMatchedId = aoRes.order?.id || null;
      }

      return res.json({
        success: true,
        duplicate: Boolean(result.duplicate),
        status: result.event.status,
        amountLkr: result.event.amountLkr,
        bank: result.event.bank,
        reference: result.event.reference,
        matchedOrderId: autoOrderMatchedId || result.matchedOrder?.id || null,
        autoOrderOutcome,
      });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Webhook processing failed' });
    }
  });

  // --- AUTO ORDER MANAGEMENT & FFGLORY API DELIVERY ROUTES ---
  app.use('/api/auto-orders', requireAuthenticatedUser, requireBusinessTier);

  const getBizActor = (req: Request): string => {
    const principal = (req as any).authPrincipal as AuthenticatedPrincipal | undefined;
    if (principal?.email) return principal.email;
    const raw = req.headers['x-user-id'] || req.query.userId || req.body?.userId;
    return typeof raw === 'string' && raw.trim() ? raw.trim().slice(0, 64) : 'admin';
  };

  app.get('/api/auto-orders/dashboard', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const forceRefresh = req.query.force === 'true';
      await ensureFreshFFGloryData(tenantId, { force: forceRefresh });
      const summary = getAutoOrderDashboardSummary(tenantId);
      const store = getBusinessStore(tenantId);
      const engine = getBotEngine(tenantId);
      res.json({
        success: true,
        data: {
          ...summary,
          botOnline: Boolean(engine.state.isOnline),
          products: store.products.map((p) => ({
            id: p.id,
            name: p.name,
            category: p.category,
            platform: p.platform,
            price: p.price,
            packages: p.packages || [],
            inStock: p.inStock,
          })),
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load Auto Order dashboard' });
    }
  });

  app.post('/api/auto-orders/settings', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const {
        ffgloryEnabled,
        operatingMode,
        autoDeliverOnVerifiedPayment,
        supportContact,
        apiKey,
        masterKey,
        clearApiKey,
        clearMasterKey,
      } = req.body || {};

      updateAutoOrderSettings(tenantId, actor, {
        ffgloryEnabled,
        operatingMode,
        autoDeliverOnVerifiedPayment,
        supportContact,
        apiKey,
        masterKey,
        clearApiKey,
        clearMasterKey,
      });

      const { creds } = resolveFFGloryCredentials(tenantId);
      let balanceSync: { success: boolean; account?: any; error?: string } | null = null;
      if (creds.apiKey && creds.masterKey) {
        balanceSync = await refreshFFGloryBalance(tenantId);
        await refreshFFGloryPricing(tenantId).catch(() => null);
        if (balanceSync?.success && (apiKey || masterKey) && operatingMode !== 'TEST') {
          updateAutoOrderSettings(tenantId, actor, { operatingMode: 'LIVE', ffgloryEnabled: true });
        }
      }

      const freshSummary = getAutoOrderDashboardSummary(tenantId);
      res.json({
        success: true,
        data: freshSummary.settings,
        balance: balanceSync?.account || null,
        connectionSuccess: balanceSync ? balanceSync.success : null,
        message: balanceSync
          ? balanceSync.success
            ? `FFGlory API Connected (LIVE MODE)! Balance: ${balanceSync.account?.basic_credits ?? 0} Basic · ${balanceSync.account?.premium_credits ?? 0} Premium Credits`
            : `Settings saved, but FFGlory connection failed: ${balanceSync.error}`
          : 'FFGlory API settings saved securely on server.',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to save API settings' });
    }
  });

  app.post('/api/auto-orders/test-connection', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const result = await refreshFFGloryBalance(tenantId);
      res.json({
        success: result.success,
        data: result.account || null,
        message: result.success
          ? `Connected to FFGlory Panel as ${result.account?.username} (Basic: ${result.account?.basic_credits}, Premium: ${result.account?.premium_credits})`
          : result.error || 'Failed to connect to FFGlory API',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Connection test error' });
    }
  });

  app.get('/api/auto-orders/balance', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const result = await refreshFFGloryBalance(tenantId);
      res.json({
        success: result.success,
        data: result.account || null,
        message: result.success ? 'Balance refreshed.' : result.error || 'Failed to fetch balance',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to check balance' });
    }
  });

  app.get('/api/auto-orders/pricing', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const result = await refreshFFGloryPricing(tenantId);
      res.json({
        success: result.success,
        data: result.pricing || null,
        message: result.success ? 'Loaded live regions & pricing from FFGlory.' : result.error || 'Failed to load pricing',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load pricing' });
    }
  });

  app.get('/api/auto-orders/ffglory/live-data', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const includeNotifications = req.query.includeNotifications === 'true';
      const { creds } = resolveFFGloryCredentials(tenantId);
      if (!creds.apiKey || !creds.masterKey) {
        return res.status(400).json({
          success: false,
          message: 'Configure FFGlory API Key and Master Key in API Settings first.',
        });
      }

      const [groupsRes, couponsRes, txnsRes, notifsRes] = await Promise.allSettled([
        ffgloryClient.getMyGroups(creds),
        ffgloryClient.getMyCoupons(creds),
        ffgloryClient.getTransactions(creds),
        includeNotifications ? ffgloryClient.getNotifications(creds) : Promise.resolve([]),
      ]);

      const state = getAutoOrderState(tenantId);
      let hasErrors = false;
      const errors: string[] = [];
      const recordErr = (endpoint: string, reason: any) => {
        hasErrors = true;
        const code = reason instanceof FFGloryApiError ? reason.statusCode : 500;
        const msg = reason?.message || 'Request failed';
        errors.push(`${endpoint}: ${msg}`);
        appendApiError(state, endpoint, code, msg);
      };

      if (groupsRes.status === 'rejected') recordErr('/api/client/my-groups', groupsRes.reason);
      if (couponsRes.status === 'rejected') recordErr('/api/client/my-coupons', couponsRes.reason);
      if (txnsRes.status === 'rejected') recordErr('/api/client/transactions', txnsRes.reason);
      if (notifsRes.status === 'rejected') recordErr('/api/client/notifications', notifsRes.reason);
      if (hasErrors) saveAutoOrderState(state);

      res.json({
        success: true,
        data: {
          groups: groupsRes.status === 'fulfilled' ? groupsRes.value : [],
          coupons: couponsRes.status === 'fulfilled' ? couponsRes.value : [],
          transactions: txnsRes.status === 'fulfilled' ? txnsRes.value : [],
          notifications: notifsRes.status === 'fulfilled' ? notifsRes.value : [],
          errors,
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load FFGlory live data' });
    }
  });

  app.post('/api/auto-orders/mappings', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const {
        productId,
        packageId,
        enabled,
        actionType,
        basicCredits,
        premiumCredits,
        region,
        priceLkrOverride,
      } = req.body || {};

      if (!productId || typeof productId !== 'string') {
        return res.status(400).json({ success: false, message: 'productId is required.' });
      }

      const mappings = upsertProductMapping(tenantId, actor, {
        productId,
        packageId,
        enabled: enabled !== false,
        actionType,
        basicCredits,
        premiumCredits,
        region,
        priceLkrOverride,
      });

      res.json({ success: true, data: mappings, message: 'Product delivery mapping saved.' });
    } catch (err: any) {
      res.status(400).json({ success: false, message: err?.message || 'Failed to save product mapping' });
    }
  });

  app.delete('/api/auto-orders/mappings', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const productId = String(req.body?.productId || req.query.productId || '').trim();
      const packageId = String(req.body?.packageId || req.query.packageId || '').trim() || undefined;
      if (!productId) {
        return res.status(400).json({ success: false, message: 'productId is required.' });
      }
      const mappings = deleteProductMapping(tenantId, actor, productId, packageId);
      res.json({ success: true, data: mappings, message: 'Mapping removed.' });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to delete mapping' });
    }
  });

  app.post('/api/auto-orders/orders/:id/approve', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const engine = getBotEngine(tenantId);
      const result = await adminApproveOrderPayment(
        tenantId,
        req.params.id,
        actor,
        engine.getAutoOrderWhatsAppDispatcher(),
        {
          transactionId: req.body?.transactionId,
          bank: req.body?.bank,
          clanId: req.body?.clanId,
        }
      );
      res.status(result.success ? 200 : 400).json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to approve payment' });
    }
  });

  app.post('/api/auto-orders/orders/:id/reject', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const result = adminRejectOrCancelOrder(tenantId, req.params.id, actor, 'REJECT', req.body?.reason);
      res.status(result.success ? 200 : 400).json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to reject payment' });
    }
  });

  app.post('/api/auto-orders/orders/:id/cancel', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const result = adminRejectOrCancelOrder(tenantId, req.params.id, actor, 'CANCEL', req.body?.reason);
      res.status(result.success ? 200 : 400).json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to cancel order' });
    }
  });

  app.post('/api/auto-orders/orders/:id/retry', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const engine = getBotEngine(tenantId);
      const result = await executeAutoOrderDelivery(tenantId, req.params.id, {
        actor,
        allowRetry: true,
        dispatcher: engine.getAutoOrderWhatsAppDispatcher(),
      });
      res.status(result.success ? 200 : 400).json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to retry delivery' });
    }
  });

  app.post('/api/auto-orders/simulate-test', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const state = getAutoOrderState(tenantId);
      if (state.settings.operatingMode !== 'TEST') {
        return res.status(400).json({
          success: false,
          message: 'Switch to TEST MODE in API Settings before running end-to-end simulations.',
        });
      }

      const {
        actionType = 'COUPON',
        customerNumber = '',
        customerName = 'Test Customer',
        amountLkr = 1500,
        basicCredits = 1,
        premiumCredits = 0,
        region = 'bd',
        clanId = '123456789',
        sendWhatsApp = false,
      } = req.body || {};

      const cleanDigits = normalizeWhatsAppNumber(String(customerNumber || '')) || String(customerNumber || '').replace(/\D/g, '');
      const customerJid = cleanDigits.length >= 8 ? `${cleanDigits}@s.whatsapp.net` : '';

      const order = createAutoOrder(tenantId, {
        customerJid,
        customerNumber: cleanDigits || '94700000000',
        customerName: String(customerName || 'Test Customer').slice(0, 60),
        productId: actionType === 'CLAN_GROUP' ? 'test-ffglory-group' : 'test-ffglory-coupon',
        productName:
          actionType === 'CLAN_GROUP'
            ? `Test Clan Glory Group (${String(region).toUpperCase()})`
            : `Test FFGlory Coupon (${basicCredits}B / ${premiumCredits}P)`,
        priceDisplay: `LKR ${Number(amountLkr) || 1500}`,
        amountLkr: Number(amountLkr) || 1500,
        clanId: actionType === 'CLAN_GROUP' ? String(clanId || '123456789') : undefined,
        regionOverride: actionType === 'CLAN_GROUP' ? String(region || 'bd') : undefined,
        modeOverride: 'TEST',
      });

      const db = getAutoOrderState(tenantId);
      const found = db.orders.find((o) => o.id === order.id);
      if (found) {
        found.deliveryAction = actionType === 'CLAN_GROUP' ? 'CLAN_GROUP' : 'COUPON';
        found.deliveryParams =
          found.deliveryAction === 'CLAN_GROUP'
            ? { region: String(region || 'bd').toLowerCase(), clan_id: String(clanId || '123456789') }
            : {
                basic_credits: Math.max(0, Math.floor(Number(basicCredits) || 0)),
                premium_credits: Math.max(0, Math.floor(Number(premiumCredits) || 0)),
              };
        saveAutoOrderState(db);
      }

      const simRef = `SIM${Math.floor(10000000 + Math.random() * 90000000)}`;
      const engine = getBotEngine(tenantId);
      const simEvent = {
        id: `sim-${Date.now()}`,
        source: 'test' as const,
        bank: 'Commercial Bank (Test)',
        amountLkr: Number(amountLkr) || 1500,
        reference: simRef,
        sender: 'COMBANK',
        rawMessage: `A/C Credited LKR ${(Number(amountLkr) || 1500).toFixed(2)} Ref ${simRef} ${order.id}`,
        status: 'matched' as const,
        timestamp: new Date().toISOString(),
      };

      const flowResult = await processGatewayPaymentForAutoOrders(
        tenantId,
        simEvent,
        sendWhatsApp ? engine.getAutoOrderWhatsAppDispatcher() : undefined,
        { targetOrderId: order.id, verifiedBy: 'TEST_SIMULATOR' }
      );

      res.json({
        success: flowResult.outcome === 'VERIFIED_AND_DELIVERED',
        data: flowResult,
        message: flowResult.message,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Test simulation failed' });
    }
  });

  app.post('/api/auto-orders/guild-settings', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const actor = getBizActor(req);
      const {
        guildFlowEnabled,
        welcomeOnFirstMessage,
        guildPricing,
        paymentAccount,
        publicPortalBaseUrl,
      } = req.body || {};
      const updated = updateGuildFlowSettings(tenantId, actor, {
        guildFlowEnabled,
        welcomeOnFirstMessage,
        guildPricing,
        paymentAccount,
        publicPortalBaseUrl,
      });
      res.json({
        success: true,
        data: updated,
        message: 'Guild Service Pricing & Payment Account Settings saved.',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to save Guild settings' });
    }
  });

  app.get('/api/auto-orders/receipt-file/:fileName', (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const resolvedPath = resolveStoredReceiptFilePath(tenantId, req.params.fileName);
      if (!resolvedPath) {
        return res.status(404).json({ success: false, message: 'Receipt file not found.' });
      }
      res.sendFile(resolvedPath);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load receipt file' });
    }
  });

  app.post('/api/auto-orders/orders/:id/live-glory', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const result = await fetchOrderLiveGlory(tenantId, req.params.id, { force: true });
      res.status(result.success ? 200 : 400).json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to refresh Live Glory' });
    }
  });

  app.post('/api/auto-orders/orders/:id/send-guild-config', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const state = getAutoOrderState(tenantId);
      const order = state.orders.find((o) => o.id === req.params.id);
      if (!order) return res.status(404).json({ success: false, message: 'Order not found.' });
      const engine = getBotEngine(tenantId);
      const sent = await sendPhase5GuildConfigCard(tenantId, order, engine.getAutoOrderWhatsAppDispatcher());
      res.json({
        success: sent,
        portalUrl: buildOrderPortalUrl(tenantId, order),
        message: sent ? 'Guild Configuration Card sent to WhatsApp.' : 'Bot offline — you can share the Portal URL directly.',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to send Guild Config Card' });
    }
  });

  app.post('/api/auto-orders/simulate-guild-flow', async (req: Request, res: Response) => {
    try {
      const tenantId = getBizStoreTenantId(req);
      const state = getAutoOrderState(tenantId);
      if (state.settings.operatingMode !== 'TEST') {
        return res.status(400).json({
          success: false,
          message: 'Switch to TEST MODE in API Settings to create interactive test Guild orders.',
        });
      }
      const rawQty = Number(req.body?.botQuantity || 4);
      const botQuantity: GuildBotQuantity =
        rawQty === 8 || rawQty === 12 || rawQty === 16 ? rawQty : 4;
      const customerName = String(req.body?.customerName || 'Test Guild Customer').slice(0, 60);
      const customerNumber = String(req.body?.customerNumber || '94700000000').replace(/\D/g, '') || '94700000000';

      const order = createGuildServiceOrder(tenantId, {
        customerJid: `${customerNumber}@s.whatsapp.net`,
        customerNumber,
        customerName,
        botQuantity,
        modeOverride: 'TEST',
      });

      const simRef = `SIMG${Math.floor(10000000 + Math.random() * 90000000)}`;
      const flowRes = await processGatewayPaymentForAutoOrders(
        tenantId,
        {
          id: `simg-${Date.now()}`,
          source: 'test',
          bank: 'Commercial Bank (Test)',
          amountLkr: order.amountLkr,
          reference: simRef,
          sender: 'COMBANK',
          rawMessage: `Credited LKR ${order.amountLkr.toFixed(2)} Ref ${simRef} ${order.id}`,
          status: 'matched',
          timestamp: new Date().toISOString(),
        },
        undefined,
        { targetOrderId: order.id, verifiedBy: 'TEST_SIMULATOR' }
      );

      const freshOrder = getAutoOrderState(tenantId).orders.find((o) => o.id === order.id) || order;
      const portalUrl = buildOrderPortalUrl(tenantId, freshOrder);
      res.json({
        success: true,
        data: {
          order: freshOrder,
          portalUrl,
          portalPath: `/guild-portal?order=${encodeURIComponent(freshOrder.id)}&token=${encodeURIComponent(freshOrder.portalToken || '')}`,
          outcome: flowRes.outcome,
        },
        message: `Test Guild Order ${freshOrder.id} (${botQuantity} BOTS · LKR ${freshOrder.amountLkr}) verified! Open the Guild Configuration Portal to test Phases 5–8.`,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Guild test simulation failed' });
    }
  });

  // --- PUBLIC TOKEN-SCOPED CUSTOMER GUILD PORTAL (PHASE 5 CONFIG + PHASE 7 PROCESSING + PHASE 8 LIVE GLORY) ---
  app.get('/api/guild-portal/:orderId', async (req: Request, res: Response) => {
    try {
      const orderId = String(req.params.orderId || '').trim().toUpperCase();
      const token = String(req.query.token || '').trim();
      const match = findOrderByPortalToken(orderId, token);
      if (!match) {
        return res.status(403).json({
          success: false,
          message: 'Invalid or expired Guild Configuration link. Please check your WhatsApp order message.',
        });
      }

      const { tenantId, order } = match;
      await ensureFreshFFGloryData(tenantId, { balanceTtlMs: 60000, pricingTtlMs: 300000 }).catch(() => {});
      const state = getAutoOrderState(tenantId);
      const liveRegions = (state.settings.cachedPricing?.regions || []).filter((r) => r.enabled !== false);

      res.json({
        success: true,
        data: {
          brandName: 'YASHI HALLS',
          order: {
            id: order.id,
            mode: order.mode,
            serviceType: order.serviceType || 'GUILD_SERVICE',
            productName: order.productName,
            botQuantity: order.botQuantity || 4,
            amountLkr: order.amountLkr,
            status: order.status,
            statusReason: order.statusReason || '',
            processingStage: order.processingStage || (order.status === 'DELIVERED' ? 'LAUNCHED' : 'PAYMENT_VERIFIED'),
            guildConfig: order.guildConfig || null,
            payment: order.payment
              ? {
                  bank: order.payment.bank,
                  amountLkr: order.payment.amountLkr,
                  transactionId: order.payment.transactionId,
                  paidAt: order.payment.paidAt,
                }
              : null,
            delivery: order.delivery
              ? {
                  success: order.delivery.success,
                  groupId: order.delivery.groupId,
                  botCount: order.delivery.botCount,
                  region: order.delivery.region,
                  clanId: order.delivery.clanId,
                  completedAt: order.delivery.completedAt,
                  errorMessage: order.delivery.errorMessage,
                }
              : null,
            liveGlory: order.liveGlory
              ? {
                  currentGlory: order.liveGlory.currentGlory,
                  clanName: order.liveGlory.clanName,
                  clanLevel: order.liveGlory.clanLevel,
                  memberCount: order.liveGlory.memberCount,
                  groupStatus: order.liveGlory.groupStatus,
                  uptime: order.liveGlory.uptime,
                  lastUpdatedAt: order.liveGlory.lastUpdatedAt,
                  errors: order.liveGlory.errors,
                }
              : null,
            createdAt: order.createdAt,
            updatedAt: order.updatedAt,
          },
          regions: liveRegions.map((r) => ({
            id: r.id,
            region_name: r.region_name,
            region_code: r.region_code,
            tier: r.tier,
            enabled: r.enabled,
          })),
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to load Guild Portal' });
    }
  });

  app.post('/api/guild-portal/:orderId/configure', async (req: Request, res: Response) => {
    try {
      const orderId = String(req.params.orderId || '').trim().toUpperCase();
      const token = String(req.body?.token || '').trim();
      const regionId = String(req.body?.regionId || '').trim();
      const clanId = String(req.body?.clanId || '').trim();

      const match = findOrderByPortalToken(orderId, token);
      if (!match) {
        return res.status(403).json({ success: false, message: 'Invalid or expired portal authorization token.' });
      }

      const engine = getBotEngine(match.tenantId);
      const result = await submitGuildConfiguration(
        match.tenantId,
        match.order.id,
        {
          regionId,
          clanId,
          submittedVia: 'WEB_CARD',
          actor: `customer:${match.order.customerNumber}`,
        },
        engine.getAutoOrderWhatsAppDispatcher()
      );

      res.status(result.success ? 200 : 400).json({
        success: result.success,
        message: result.message,
        orderStatus: result.order?.status || null,
        groupId: result.order?.delivery?.groupId || null,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to submit Guild Details' });
    }
  });

  app.post('/api/guild-portal/:orderId/live-glory', async (req: Request, res: Response) => {
    try {
      const orderId = String(req.params.orderId || '').trim().toUpperCase();
      const token = String(req.body?.token || '').trim();
      const match = findOrderByPortalToken(orderId, token);
      if (!match) {
        return res.status(403).json({ success: false, message: 'Invalid or expired portal authorization token.' });
      }

      const result = await fetchOrderLiveGlory(match.tenantId, match.order.id, { force: false });
      res.status(result.success ? 200 : 400).json({
        success: result.success,
        message: result.message,
        cooldownRemainingSec: result.cooldownRemainingSec || 0,
        liveGlory: result.liveGlory
          ? {
              currentGlory: result.liveGlory.currentGlory,
              clanName: result.liveGlory.clanName,
              clanLevel: result.liveGlory.clanLevel,
              memberCount: result.liveGlory.memberCount,
              groupStatus: result.liveGlory.groupStatus,
              uptime: result.liveGlory.uptime,
              lastUpdatedAt: result.liveGlory.lastUpdatedAt,
              errors: result.liveGlory.errors,
            }
          : null,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fetch Live Glory' });
    }
  });

  // Get registered plugins and provider status
  app.get('/api/plugins', (req: Request, res: Response) => {
    res.json({
      success: true,
      data: getPluginStatusList(),
    });
  });

  // Server-side AI Chat with Gemini API (protects API Key)
  app.post('/api/ai/chat', async (req: Request, res: Response) => {
    try {
      const { prompt, context } = req.body;
      if (!prompt || typeof prompt !== 'string') {
        return res.status(400).json({ success: false, message: 'Prompt is required' });
      }

      const result = await askGemini(prompt, context);
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'AI request failed' });
    }
  });

  const verifyAdmin = requireAuthorizedAdmin;

  // --- MANUAL PURCHASE REQUESTS (CUSTOMER & ADMIN) ---

  // Customer submits a manual purchase request (Coin packs or legacy manual requests)
  app.post('/api/purchase-requests/create', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const {
        itemType,
        itemName,
        planId,
        coinAmount,
        priceLkr,
        priceFormatted,
      } = req.body;

      if (!itemType || !itemName) {
        return res.status(400).json({
          success: false,
          message: 'Missing required purchase request details.',
        });
      }

      const newReq = adminStore.createPurchaseRequest({
        customerId: principal.uid,
        customerEmail: principal.email,
        customerName: principal.username || principal.email.split('@')[0],
        itemType,
        itemName,
        planId,
        coinAmount: Number(coinAmount) || undefined,
        priceLkr: Number(priceLkr) || 0,
        priceFormatted: priceFormatted || `Rs ${Number(priceLkr || 0).toLocaleString()}`,
      });

      adminStore.syncCustomer(
        newReq.customerId,
        newReq.customerEmail,
        newReq.customerName
      );

      res.json({
        success: true,
        request: newReq,
        adminWhatsapp: adminStore.getSettings().adminWhatsapp,
        message: 'Manual purchase request registered successfully. Status: PENDING ADMIN APPROVAL.',
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to submit purchase request' });
    }
  });

  // Get user's own purchase requests
  app.get('/api/purchase-requests/user/:email', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const email = String(req.params.email).trim().toLowerCase();
      if (!principal.isAdmin && principal.email !== email) {
        return res.status(403).json({ success: false, message: 'Forbidden.' });
      }
      const all = adminStore.getPurchaseRequests();
      const userRequests = all.filter((r) => r.customerEmail.toLowerCase() === email);
      res.json({ success: true, data: userRequests });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fetch requests' });
    }
  });

  // Get live customer profile by email (used by client to sync approved coins and plans)
  app.get('/api/customer/profile/:email', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const email = String(req.params.email || '').trim().toLowerCase();
      if (!principal.isAdmin && principal.email !== email) {
        return res.status(403).json({ success: false, message: 'Forbidden.' });
      }
      const cust = adminStore.getCustomer(email);
      if (!cust) {
        return res.json({ success: false, message: 'Customer not found' });
      }
      const subSummary = subscriptionService.getUserSubscriptionView(
        principal.uid,
        principal.email,
        principal.username
      ).subscription;
      res.json({
        success: true,
        customer: {
          ...cust,
          plan: subSummary.plan,
          planActivatedAt: subSummary.activatedAt || cust.planActivatedAt,
          planExpiresAt: subSummary.expiresAt || cust.planExpiresAt,
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Error' });
    }
  });

  // --- ADMIN PROTECTED ROUTES ---

  // Verify Admin Permissions
  app.post('/api/admin/verify', verifyAdmin, (req: Request, res: Response) => {
    res.json({
      success: true,
      isAdmin: true,
      email: (req as any).adminEmail,
      message: 'Verified Master Administrator privileges.',
    });
  });

  // Admin Overview Stats
  app.get('/api/admin/overview', verifyAdmin, (req: Request, res: Response) => {
    try {
      const stats = adminStore.getOverviewStats();
      const botState = botEngine.getState();
      res.json({
        success: true,
        data: {
          ...stats,
          botConnectionStatus: botState.connectionStatus,
          isBotOnline: botState.isOnline,
          pairedNumber: botState.pairedNumber,
          engineUptimeSeconds: botState.uptimeSeconds,
        },
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get stats' });
    }
  });

  // Admin Customers list
  app.get('/api/admin/customers', verifyAdmin, (req: Request, res: Response) => {
    try {
      const customers = adminStore.getCustomers();
      res.json({ success: true, data: customers });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get customers' });
    }
  });

  // Admin Paired WhatsApp Bots & Connected Accounts list
  app.get('/api/admin/paired-bots', verifyAdmin, (req: Request, res: Response) => {
    try {
      const sessionsDir = path.resolve(process.cwd(), 'data', 'sessions');
      const pairedList: any[] = [];
      const sessionFolderNames = fs.existsSync(sessionsDir)
        ? fs.readdirSync(sessionsDir).filter((f) => {
            try {
              return fs.statSync(path.join(sessionsDir, f)).isDirectory();
            } catch {
              return false;
            }
          })
        : [];

      const backups = adminStore.getSessionBackups() || {};
      const allIds = Array.from(new Set<string>([...sessionFolderNames, ...Object.keys(backups)]));

      for (const id of allIds) {
        const engine = getBotEngine(id);
        const state = engine.getState();
        const credsPath = path.join(sessionsDir, id, 'whatsapp_session', 'creds.json');
        let filePhone: string | null = null;
        let isRegistered = false;
        let lastModified: string | null = null;

        if (fs.existsSync(credsPath)) {
          try {
            const stat = fs.statSync(credsPath);
            lastModified = stat.mtime.toISOString();
            const raw = fs.readFileSync(credsPath, 'utf-8');
            const parsed = JSON.parse(raw);
            if (parsed && (parsed.registered || parsed.me?.id)) {
              isRegistered = true;
              filePhone = parsed.me?.id ? parsed.me.id.split(':')[0].split('@')[0] : null;
            }
          } catch {}
        }

        const backupInfo = backups[id];
        const displayPhone = state.pairedNumber || filePhone || backupInfo?.phoneNumber || (id.match(/^\d+$/) ? id : 'Unknown');

        pairedList.push({
          id,
          sessionId: id,
          pairedNumber: displayPhone,
          connectionStatus: state.connectionStatus,
          isOnline: state.isOnline,
          isRegistered: isRegistered || Boolean(backupInfo),
          plan: state.config.plan || 'mini',
          botName: state.config.botName || 'YASHI HALLS',
          ownerNumber: state.config.ownerNumber || '94740712934',
          uptimeSeconds: state.uptimeSeconds,
          lastActive: lastModified || backupInfo?.timestamp || new Date().toISOString(),
          totalMessagesReceived: state.totalMessagesReceived || 0,
          totalCommandsExecuted: state.totalCommandsExecuted || 0,
        });
      }

      res.json({ success: true, count: pairedList.length, data: pairedList });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fetch paired bots' });
    }
  });

  // Sync/register authenticated customer (non-admins cannot self-elevate plan or coins)
  app.post('/api/admin/customers/sync', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const { id, email, username, plan, coins } = req.body || {};

      if (principal.isAdmin) {
        if (!email) return res.status(400).json({ success: false, message: 'Email required' });
        const record = adminStore.syncCustomer(id, email, username, plan, coins);
        return res.json({ success: true, data: record });
      }

      const effectiveTier = subscriptionService.getEffectiveTier(principal.uid, principal.email, false);
      const record = adminStore.syncCustomer(
        principal.uid,
        principal.email,
        username || principal.username,
        effectiveTier
      );
      return res.json({ success: true, data: record });
    } catch (err: any) {
      return res.status(500).json({ success: false, message: err?.message || 'Failed to sync' });
    }
  });

  // Admin Suspend / Unsuspend Customer
  app.post('/api/admin/customers/suspend', verifyAdmin, (req: Request, res: Response) => {
    try {
      const { customerId, isSuspended } = req.body;
      const adminEmail = (req as any).adminEmail;
      const updated = adminStore.setCustomerSuspended(customerId, Boolean(isSuspended), adminEmail);
      if (!updated) return res.status(404).json({ success: false, message: 'Customer not found' });
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update' });
    }
  });

  // Admin Update Customer Note
  app.post('/api/admin/customers/note', verifyAdmin, (req: Request, res: Response) => {
    try {
      const { customerId, note } = req.body;
      const adminEmail = (req as any).adminEmail;
      const updated = adminStore.updateCustomerNote(customerId, String(note || ''), adminEmail);
      if (!updated) return res.status(404).json({ success: false, message: 'Customer not found' });
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update note' });
    }
  });

  // Admin Delete Customer
  app.delete('/api/admin/customers/:id', verifyAdmin, (req: Request, res: Response) => {
    try {
      const customerId = req.params.id;
      const adminEmail = (req as any).adminEmail;
      const ok = adminStore.deleteCustomer(customerId, adminEmail);
      if (!ok) return res.status(404).json({ success: false, message: 'Customer not found' });
      res.json({ success: true, message: 'Customer permanently deleted.' });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to delete customer' });
    }
  });

  // Admin Modify Coins (Add / Deduct)
  app.post('/api/admin/coins/adjust', verifyAdmin, (req: Request, res: Response) => {
    try {
      const { customerId, action, amount, reason } = req.body;
      const adminEmail = (req as any).adminEmail;

      if (action !== 'ADD' && action !== 'DEDUCT') {
        return res.status(400).json({ success: false, message: 'Action must be "ADD" or "DEDUCT".' });
      }

      const numAmount = Number(amount);
      if (isNaN(numAmount) || numAmount <= 0) {
        return res.status(400).json({ success: false, message: 'Amount must be a positive number.' });
      }

      const result = adminStore.modifyCoins(customerId, action, numAmount, reason, adminEmail);
      if (!result.success) {
        return res.status(400).json(result);
      }
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to adjust coins' });
    }
  });

  // Admin View Purchase Requests
  app.get('/api/admin/purchase-requests', verifyAdmin, (req: Request, res: Response) => {
    try {
      const status = req.query.status as string;
      const list = adminStore.getPurchaseRequests(status);
      res.json({ success: true, data: list });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fetch purchase requests' });
    }
  });

  // Admin Fulfill / Approve / Reject Purchase Request
  app.post('/api/admin/purchase-requests/fulfill', verifyAdmin, (req: Request, res: Response) => {
    try {
      const { requestId, action, adminNote } = req.body;
      const adminEmail = (req as any).adminEmail;

      if (action !== 'APPROVE' && action !== 'REJECT') {
        return res.status(400).json({ success: false, message: 'Action must be APPROVE or REJECT' });
      }

      const result = adminStore.fulfillPurchaseRequest(requestId, action, adminEmail, adminNote);
      if (!result.success) {
        return res.status(400).json(result);
      }
      res.json(result);
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fulfill request' });
    }
  });

  // Admin View Audit Logs
  app.get('/api/admin/audit-logs', verifyAdmin, (req: Request, res: Response) => {
    try {
      const logs = adminStore.getAuditLogs();
      res.json({ success: true, data: logs });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get audit logs' });
    }
  });

  // Admin System Settings (Get & Update)
  app.get('/api/admin/settings', verifyAdmin, (req: Request, res: Response) => {
    try {
      const settings = adminStore.getSettings();
      res.json({ success: true, data: settings });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get settings' });
    }
  });

  app.post('/api/admin/settings', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const updated = adminStore.updateSettings(req.body, adminEmail);
      res.json({ success: true, data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update settings' });
    }
  });

  // Public Site Design Endpoint (used by Web Frontend & CSS Injector)
  app.get('/api/site-design', (_req: Request, res: Response) => {
    try {
      const design = adminStore.getSiteDesign();
      res.json({ success: true, data: design });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get site design' });
    }
  });

  // Admin Site Design & CSS Studio (Get & Update)
  app.get('/api/admin/site-design', verifyAdmin, (_req: Request, res: Response) => {
    try {
      const design = adminStore.getSiteDesign();
      res.json({ success: true, data: design });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get site design' });
    }
  });

  app.post('/api/admin/site-design', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const updated = adminStore.updateSiteDesign(req.body, adminEmail);
      res.json({ success: true, data: updated, message: 'Website Design & CSS updated successfully!' });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update site design' });
    }
  });

  // Admin Bot Studio & Commands Engine (Get & Update)
  app.get('/api/admin/bot-studio', verifyAdmin, (_req: Request, res: Response) => {
    try {
      const studio = adminStore.getBotStudio();
      res.json({ success: true, data: studio });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get bot studio' });
    }
  });

  app.post('/api/admin/bot-studio', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const updated = adminStore.updateBotStudio(req.body, adminEmail);
      
      // Hot-apply changes to active BotEngine
      try {
        const engine = getEngineForReq(req);
        engine.updateConfig({
          botName: updated.botName,
          botLogoUrl: updated.botLogoUrl,
          prefix: updated.prefix,
          ownerNumber: updated.ownerNumber,
          ...(updated.featureFlags || {})
        });
      } catch (engineErr) {
        console.warn('[AppServer] Hot-sync to active bot engine warning:', engineErr);
      }

      res.json({ success: true, data: updated, message: 'Bot Engine & Commands Studio saved successfully!' });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update bot studio' });
    }
  });

  // 1. Audio Streaming Endpoint (supports HTTP 206 Partial Content Range for smooth browser playback & seeking)
  app.get('/api/voice-replies/audio/:id', (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const voiceReply = adminStore.getVoiceReply(id);
      if (!voiceReply) {
        return res.status(404).json({ success: false, message: 'Voice Reply record not found' });
      }

      const streamInfo = getAudioFileStream(voiceReply.audioPath);
      if (!streamInfo || !fs.existsSync(streamInfo.filePath)) {
        return res.status(404).json({ success: false, message: 'Audio file missing from storage' });
      }

      const stat = fs.statSync(streamInfo.filePath);
      const totalSize = stat.size;
      const range = req.headers.range;

      if (range) {
        const parts = range.replace(/bytes=/, '').split('-');
        const start = parseInt(parts[0], 10);
        const end = parts[1] ? parseInt(parts[1], 10) : totalSize - 1;
        const chunkSize = end - start + 1;
        const fileStream = fs.createReadStream(streamInfo.filePath, { start, end });

        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${totalSize}`,
          'Accept-Ranges': 'bytes',
          'Content-Length': chunkSize,
          'Content-Type': streamInfo.mimeType,
        });
        fileStream.pipe(res);
      } else {
        res.writeHead(200, {
          'Content-Length': totalSize,
          'Content-Type': streamInfo.mimeType,
          'Accept-Ranges': 'bytes',
        });
        fs.createReadStream(streamInfo.filePath).pipe(res);
      }
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Streaming audio failed' });
    }
  });

  // 2. Admin Get All Voice Replies
  app.get('/api/admin/voice-replies', verifyAdmin, (req: Request, res: Response) => {
    try {
      const scope = req.query.scope as string;
      const list = adminStore.getVoiceReplies(scope);
      res.json({ success: true, data: list });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to get voice replies' });
    }
  });

  // 3. Admin Create & Upload Voice Reply
  app.post('/api/admin/voice-replies/create', verifyAdmin, async (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const {
        name,
        triggers,
        audioBase64,
        audioFileName,
        language = 'Both',
        scope = 'GLOBAL',
        enabled = true,
        customerId,
        customerEmail,
      } = req.body;

      if (!name || typeof name !== 'string' || !name.trim()) {
        return res.status(400).json({ success: false, message: 'Reply Name is required.' });
      }

      let parsedTriggers: string[] = [];
      if (Array.isArray(triggers)) {
        parsedTriggers = triggers.map((t) => String(t).trim()).filter(Boolean);
      } else if (typeof triggers === 'string') {
        parsedTriggers = triggers
          .split(/[,;\n]/)
          .map((t) => t.trim())
          .filter(Boolean);
      }

      if (parsedTriggers.length === 0) {
        return res.status(400).json({
          success: false,
          message: 'At least one Trigger Message is required (e.g. "hi, hello, හායි").',
        });
      }

      if (!audioBase64 || !audioFileName) {
        return res.status(400).json({ success: false, message: 'Audio file (MP3/M4A/WAV/OGG) is required.' });
      }

      // Decode base64
      const base64Data = audioBase64.replace(/^data:audio\/[a-zA-Z0-9_-]+;base64,/, '');
      const buffer = Buffer.from(base64Data, 'base64');

      const validation = validateAudioBuffer(buffer, audioFileName);
      if (!validation.valid) {
        return res.status(400).json({ success: false, message: validation.error || 'Invalid audio file.' });
      }

      const prefixId = `vr_${Date.now()}`;
      const saved = await saveAudioFile(buffer, audioFileName, prefixId);

      const record = adminStore.createVoiceReply(
        {
          name: name.trim(),
          triggers: parsedTriggers,
          audioFileName: saved.fileName,
          audioPath: saved.filePath,
          mimeType: saved.mimeType,
          fileSizeBytes: saved.size,
          fileSizeFormatted: formatBytes(saved.size),
          language,
          enabled: Boolean(enabled),
          scope: scope === 'CUSTOMER' ? 'CUSTOMER' : 'GLOBAL',
          customerId: customerId || undefined,
          customerEmail: customerEmail || undefined,
        },
        adminEmail
      );

      res.json({
        success: true,
        message: `Voice Reply '${record.name}' saved and connected to WhatsApp Bot successfully!`,
        data: record,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to save voice reply' });
    }
  });

  // 4. Admin Update Voice Reply Details
  app.put('/api/admin/voice-replies/:id', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const { id } = req.params;
      const { name, triggers, language, scope, enabled } = req.body;

      let parsedTriggers: string[] | undefined = undefined;
      if (Array.isArray(triggers)) {
        parsedTriggers = triggers.map((t) => String(t).trim()).filter(Boolean);
      } else if (typeof triggers === 'string') {
        parsedTriggers = triggers.split(/[,;\n]/).map((t) => t.trim()).filter(Boolean);
      }

      const updated = adminStore.updateVoiceReply(
        id,
        {
          ...(name ? { name: String(name).trim() } : {}),
          ...(parsedTriggers ? { triggers: parsedTriggers } : {}),
          ...(language ? { language } : {}),
          ...(scope ? { scope } : {}),
          ...(typeof enabled === 'boolean' ? { enabled } : {}),
        },
        adminEmail
      );

      if (!updated) {
        return res.status(404).json({ success: false, message: 'Voice Reply not found' });
      }

      res.json({ success: true, message: 'Voice Reply updated successfully.', data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to update' });
    }
  });

  // 5. Admin Toggle Enabled
  app.post('/api/admin/voice-replies/:id/toggle', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const { id } = req.params;
      const { enabled } = req.body;
      const updated = adminStore.toggleVoiceReply(id, Boolean(enabled), adminEmail);
      if (!updated) {
        return res.status(404).json({ success: false, message: 'Voice Reply not found' });
      }
      res.json({
        success: true,
        data: updated,
        message: `Voice Reply is now ${updated.enabled ? 'Enabled' : 'Disabled'}.`,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to toggle status' });
    }
  });

  // 6. Admin Replace Audio File
  app.post('/api/admin/voice-replies/:id/replace-audio', verifyAdmin, async (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const { id } = req.params;
      const { audioBase64, audioFileName } = req.body;

      const current = adminStore.getVoiceReply(id);
      if (!current) {
        return res.status(404).json({ success: false, message: 'Voice Reply not found' });
      }

      if (!audioBase64 || !audioFileName) {
        return res.status(400).json({ success: false, message: 'New audio file payload is required' });
      }

      const base64Data = audioBase64.replace(/^data:audio\/[a-zA-Z0-9_-]+;base64,/, '');
      const buffer = Buffer.from(base64Data, 'base64');
      const validation = validateAudioBuffer(buffer, audioFileName);
      if (!validation.valid) {
        return res.status(400).json({ success: false, message: validation.error || 'Invalid audio file.' });
      }

      const prefixId = `vr_${Date.now()}`;
      const saved = await saveAudioFile(buffer, audioFileName, prefixId);

      // Delete old file
      deleteAudioFile(current.audioPath);

      const updated = adminStore.updateVoiceReply(
        id,
        {
          audioFileName: saved.fileName,
          audioPath: saved.filePath,
          mimeType: saved.mimeType,
          fileSizeBytes: saved.size,
          fileSizeFormatted: formatBytes(saved.size),
        },
        adminEmail
      );

      res.json({ success: true, message: 'Audio file replaced successfully.', data: updated });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to replace audio' });
    }
  });

  // 7. Admin Delete Voice Reply
  app.delete('/api/admin/voice-replies/:id', verifyAdmin, (req: Request, res: Response) => {
    try {
      const adminEmail = (req as any).adminEmail;
      const { id } = req.params;
      const current = adminStore.getVoiceReply(id);
      if (!current) {
        return res.status(404).json({ success: false, message: 'Voice Reply not found' });
      }

      deleteAudioFile(current.audioPath);
      adminStore.deleteVoiceReply(id, adminEmail);

      res.json({ success: true, message: `Voice Reply '${current.name}' deleted successfully.` });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to delete voice reply' });
    }
  });

  // 8. Admin Test Trigger Match Simulator
  app.post('/api/admin/voice-replies/test-trigger', verifyAdmin, (req: Request, res: Response) => {
    try {
      const { message, customerId } = req.body;
      const result = adminStore.findMatchingVoiceReply(String(message || ''), customerId);
      res.json({
        success: true,
        matched: Boolean(result.match),
        match: result.match,
        matchedTrigger: result.matchedTrigger,
        inputMessage: message,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Test trigger failed' });
    }
  });

  // 9. Customer-facing Voice Replies and Bot Toggle
  app.get('/api/voice-replies/customer', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const principal = (req as any).authPrincipal as AuthenticatedPrincipal;
      const engine = getEngineForReq(req);
      const list = adminStore.getVoiceReplies(undefined, principal.uid);
      res.json({
        success: true,
        data: list,
        voiceAutoReplyEnabled: engine.getState().config.voiceAutoReplyEnabled,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to fetch customer voice replies' });
    }
  });

  app.post('/api/voice-replies/customer/toggle', requireAuthenticatedUser, (req: Request, res: Response) => {
    try {
      const { enabled } = req.body;
      const engine = getEngineForReq(req);
      const updated = engine.updateConfig({ voiceAutoReplyEnabled: Boolean(enabled) });
      res.json({ success: true, voiceAutoReplyEnabled: updated.voiceAutoReplyEnabled });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Failed to toggle voice auto reply' });
    }
  });

  // --- INTERACTIVE NEON WEB MENU UI ---
  app.get(['/test1', '/test1.html', '/menu/test1'], (req: Request, res: Response) => {
    const test1FilePath = path.resolve(rootDir, 'public', 'test1.html');
    if (fs.existsSync(test1FilePath)) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.sendFile(test1FilePath);
    } else {
      res.status(404).send('Test1 Menu UI not found');
    }
  });

  app.get(['/menu', '/menu.html'], (req: Request, res: Response) => {
    const menuFilePath = path.resolve(rootDir, 'public', 'menu.html');
    if (fs.existsSync(menuFilePath)) {
      res.sendFile(menuFilePath);
    } else {
      res.status(404).send('Menu UI not found');
    }
  });

  app.get(['/interactive-menu', '/interactive-menu.html', '/menutest'], (req: Request, res: Response) => {
    const candidates = [
      path.resolve(rootDir, 'public', 'interactive-menu.html'),
      path.resolve(rootDir, 'dist', 'interactive-menu.html'),
      path.resolve(process.cwd(), 'public', 'interactive-menu.html'),
      path.resolve(process.cwd(), 'dist', 'interactive-menu.html'),
    ];
    for (const candidatePath of candidates) {
      if (fs.existsSync(candidatePath)) {
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        return res.sendFile(candidatePath);
      }
    }
    res.status(404).send('Interactive Menu UI not found');
  });

  // Direct Menu Command Dispatcher from WhatsApp In-App Webview
  app.post('/api/bot/execute-menu-command', async (req: Request, res: Response) => {
    try {
      const { command, jid, phone, userId } = req.body;
      if (!command) {
        return res.status(400).json({ success: false, message: 'Command is required' });
      }
      const engine = getBotEngine(userId || 'primary');
      if (!engine || !engine.sock) {
        return res.status(503).json({ success: false, message: 'WhatsApp Bot engine is not currently connected' });
      }
      const targetJid = jid || (phone ? `${phone.replace(/\D/g, '')}@s.whatsapp.net` : null) || (engine.state.pairedNumber ? `${engine.state.pairedNumber}@s.whatsapp.net` : null);
      if (!targetJid) {
        return res.status(400).json({ success: false, message: 'Target WhatsApp recipient JID is missing' });
      }

      const prefix = engine.state.config.prefix || '.';
      const cleanCmd = command.startsWith(prefix) ? command : `${prefix}${command.replace(/^[./!#]/, '')}`;

      const syntheticMsg = {
        key: {
          remoteJid: targetJid,
          fromMe: false,
          id: `MENU_WEB_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
          participant: targetJid,
        },
        message: {
          conversation: cleanCmd,
        },
        pushName: 'Interactive Menu User',
        messageTimestamp: Math.floor(Date.now() / 1000),
      };

      engine.addLog('INFO', `⚡ [INTERACTIVE MENU TRIGGER] Incoming webview command '${cleanCmd}' for ${targetJid.split('@')[0]}`);
      // Process command directly inside engine
      await engine.handleIncomingMessage(syntheticMsg);

      res.json({
        success: true,
        message: `Command '${cleanCmd}' executed successfully`,
        jid: targetJid,
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: err?.message || 'Execution failed' });
    }
  });

  // --- GOOGLE ADSENSE ADS.TXT ENDPOINT ---
  app.get('/ads.txt', (_req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send('google.com, pub-7061710490259174, DIRECT, f08c47fec0942fa0\n');
  });

  // --- VITE DEV MIDDLEWARE OR PRODUCTION STATIC SERVING ---
  const isProduction = process.env.NODE_ENV === 'production';
  const distPath = path.resolve(rootDir, 'dist');
  const hasDist = fs.existsSync(path.join(distPath, 'index.html'));

  if (!isProduction) {
    const vite = await createViteServer({
      server: { middlewareMode: true, hmr: false },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(distPath));
    app.get('*', (req: Request, res: Response, next) => {
      if (req.path.startsWith('/api/')) return next();
      const indexPath = path.resolve(distPath, 'index.html');
      if (fs.existsSync(indexPath)) {
        res.sendFile(indexPath);
      } else {
        res.status(404).send('Build output dist not found');
      }
    });
  }

  app.listen(Number(PORT), "0.0.0.0", () => {
    console.log(`🚀 [YASHI HALLS SERVER] Running on port ${PORT} (http://localhost:${PORT})`);
    console.log(`🤖 [BOT ENGINE] Ready to pair and host WhatsApp Multi-Device instance.`);

    // 1. Auto-verify & provision standalone audio/media engine binary
    ensureYtDlpBinary().catch((err) => {
      console.warn("[AUDIO ENGINE] yt-dlp init notice:", err?.message || err);
    });

    // 2. Initial temp garbage collection sweep
    cleanStaleTempFiles();

    // 3. Auto-resume all existing registered WhatsApp bot sessions (24/7 Always-On)
    autoBootAllRegisteredBots().catch((err) => {
      console.warn("[BOT ENGINE] autoBoot notice:", err?.message || err);
    });

    // 4. Subscription Auto-Expiry Downgrade & Renewal Reminder Sweep (every 60s + on boot)
    try {
      subscriptionService.runExpiryAndReminderSweep();
    } catch (err) {
      console.warn("[SUBSCRIPTION] initial sweep notice:", err);
    }
    setInterval(() => {
      try {
        subscriptionService.runExpiryAndReminderSweep();
      } catch {}
    }, 60000);

    // 5. Built-in Internal 24/7 Anti-Sleep Self-Heartbeat Engine
    // Keeps Node.js process & Cloud container awake continuously without external tools like UptimeRobot
    setInterval(async () => {
      try {
        const portNum = Number(PORT) || 3000;
        const localTarget = `http://127.0.0.1:${portNum}/api/keepalive`;
        await fetch(localTarget, { headers: { 'User-Agent': 'YashiHalls-Internal-KeepAlive/1.0' } }).catch(() => {});

        const pubUrl = process.env.PUBLIC_URL || process.env.APP_URL;
        if (pubUrl && typeof pubUrl === 'string' && pubUrl.startsWith('http')) {
          const cleanPub = pubUrl.endsWith('/') ? pubUrl.slice(0, -1) : pubUrl;
          await fetch(`${cleanPub}/api/keepalive`, { headers: { 'User-Agent': 'YashiHalls-External-KeepAlive/1.0' } }).catch(() => {});
        }
      } catch (err) {}
    }, 15000);
  });
}

export { startServer };

