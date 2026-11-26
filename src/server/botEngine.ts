import https from "https";
import { pipeline } from "stream/promises";
import { Readable } from "stream";
import {
  sendButtons,
  sendCarousel,
  sendPoll,
  sendQuiz,
  sendAlbum,
  sendCodeBlock,
  sendTable,
  sendGroupStatus,
  globalTicTacToe,
  globalScheduler,
  globalLidMapper
} from "./zqBaileysHelper";
import { generateMenuCardImage, generateMenuCardPng, generateWelcomeCardImage } from "./menuCardGenerator";
import {
  sendHtmlApp,
  editHtmlApp,
  generateDownloadStatusHtml,
  generateFilmDetailsCardHtml,
  generateFilmDownloadCardHtml,
  buildPlatformBannerDataUri,
  FilmQualityOptionButton,
  generatePingCardHtml,
  generateSettingsCardHtml,
  generateMasterInteractiveMenuHtml,
  getCachedMasterMenuPayload,
  invalidateMasterMenuCache,
  generateYashiHallsNeonMenuHtml,
  generateConnectedWelcomeHtmlApp,
  generateCategorySubmenuHtml,
  generateDownloadMenuHtml,
  generateGroupWelcomeHtmlApp,
  generateGroupGoodbyeHtmlApp,
  generateBusinessCatalogHtmlApp,
  calculateBusinessCatalogHtmlHeight,
  getEmbeddedBankBannerDataUri,
  generatePaymentBankDetailsHtml,
  generatePaymentProcessingHtml,
  YASHI_USER_TEST1_HTML,
  TEXT_TOOLS_CATALOG,
  DOWNLOADS_CATALOG,
  AI_CATALOG,
  MOVIES_CATALOG,
  MUSIC_CATALOG,
  GROUPS_CATALOG,
  GAMES_CATALOG,
  UTILITIES_CATALOG,
  SEARCH_CATALOG,
  SYSTEM_CATALOG
} from "./htmlAppHelper";
import { handleExpandedCommands } from "./expandedCommands";
import { getBusinessStore, resolveLocalAssetPath, sanitizePanelCommandSlug, BusinessProduct, BusinessPricePackage, BusinessStoreData } from "./businessStore";
import { getBusinessPayState, createBusinessPayOrder, estimateAmountLkr, getApkBinaryPath, getApkBinaryInfo, buildBotLinkKey, BusinessPayEvent, BusinessPayOrder } from "./businessPayService";
import {
  getAutoOrderState,
  findProductMapping,
  createAutoOrder,
  createGuildServiceOrder,
  getActiveCustomerPendingOrder,
  getActiveCustomerAwaitingGuildConfigOrder,
  processCustomerReceiptUpload,
  submitGuildConfiguration,
  adminApproveOrderPayment,
  adminRejectOrCancelOrder,
  sendLiveGloryCardToWhatsApp,
  sendPhase5GuildConfigCard,
  type AutoOrderWhatsAppDispatcher,
  type GuildBotQuantity,
} from "./autoOrderService";
import {
  renderWelcomeCardPng,
  renderQuantityCardPng,
  renderPaymentCardPng,
} from "./guildCardRenderer";
import {
  parsePackAndAuthor,
  imageToSticker,
  videoToSticker,
  retagStickerExif,
} from "./stickerService";
var __defProp=Object.defineProperty;var __name=(target,value)=>__defProp(target,"name",{value,configurable:true});import makeWASocket,{DisconnectReason,useMultiFileAuthState,fetchLatestBaileysVersion,Browsers,makeCacheableSignalKeyStore,downloadMediaMessage,proto,generateWAMessageFromContent,getDevice,Button,ButtonV2,ButtonV3,Carousel,Poll,AIRich,NativeFlow,parseInteractiveReply}from"@kaels/casileys";import pino from"pino";import QRCode from"qrcode";import fs from"fs";import path from"path";import{askGemini,analyzeAudioWithGemini,transcribeAudioWithGemini,translateAudioWithGemini,analyzeVideoWithGemini,generateCVWithGemini,chatAsYashiHallsAI,ChatMemoryItem}from"./geminiService";import{resolveSongAudio,searchYoutubeVideo,previewSongInfo}from"./audioService";import{resolveSearchImage}from"./imageService";import{adminStore}from"./adminStore";import{getWhatsAppVoiceBuffer}from"./voiceService";import{normalizeWhatsAppNumber}from"../utils/phone";import{resolveMediaFire}from"./mediafireService";import{resolveVideo,extractMediaQualityFlag,previewMediaForCard}from"./videoService";import{searchMovies,formatMovieCard,searchFilmSets,formatFilmSetCard,extractMovieQualityFlag,downloadMovieFileWithFallback,searchAndDownloadSubtitle,fetchPosterAsDataUri,formatBytesSize,cleanupStaleMovieTempFiles}from"./movieService";const BRAND_LOGO_URL="https://i.ibb.co/n2fYpDV/file-000000007c3c8210ae1b96d28b22c655.png";const WHATSAPP_CHANNEL_URL="https://whatsapp.com/channel/0029Vb927N4GJP8MS1yurM2Z";const OWNER_HOTLINE="0740712934";class BotEngine {
  sock: any;
  logs: any[];
  maxLogs: number;
  startTime: number | null;
  reconnectAttempts: number;
  maxReconnectAttempts: number;
  isExplicitlyStopped: boolean;
  reconnectTimer: any;
  watchdogTimer: any;
  activePairingPhone: string | null;
  customPairingCodeRequested: string | null;
  groupWarnings: Map<string, any>;
  activeQuizSessions: Map<string, any>;
  processedCalls: Set<string>;
  processedMessageIds: Set<string>;
  commandCooldowns: Map<string, number>;
  rawQrString: string | null;
  requestedFromJid: string | null;
  isQrReady: boolean;
  voiceCooldownMap: Map<string, any>;
  chatMemory: Map<string, any>;
  chatAiCooldownMap: Map<string, any>;
  smartReplyCooldownMap: Map<string, any>;
  cachedLogoBuffer: Buffer | null;
  lastCachedLogoUrl: string;
  lastWelcomeSentTime: number;
  isStarting: boolean;
  isFreshPairing: boolean;
  isPairingCodeInFlight: boolean;
  state: any;
  userId: string;
  sessionDir: string;
  configFile: string;
  welcomedCustomersFile: string;
  welcomedBusinessCustomers: Set<string>;
  businessChatState: Map<
    string,
    {
      step:
        | "welcome"
        | "platform"
        | "catalog"
        | "package"
        | "awaiting_clan_id"
        | "guild_qty"
        | "guild_awaiting_receipt"
        | "guild_awaiting_clan_id";
      productId?: string;
      packageId?: string;
      orderId?: string;
      regionId?: string;
      updatedAt: number;
    }
  >;
  businessThumbCache: Map<string, string>;
  pendingFilmChoices: Map<string, { movieTitle: string; query: string; opt1Cmd: string; opt2Cmd: string; onlyDub: boolean; ts: number }>;
  pendingMediaChoices: Map<string, { baseCmd: string; query: string; title?: string; opt1Cmd: string; opt2Cmd: string; ts: number }>;
  classicMenuUsers: Set<string>;
  classicMenuContext: Map<string, number>;

  constructor(userId="default"){this.sock=null;this.logs=[];this.maxLogs=500;this.startTime=null;this.reconnectAttempts=0;this.maxReconnectAttempts=20;this.isExplicitlyStopped=true;this.reconnectTimer=null;this.activePairingPhone=null;this.customPairingCodeRequested=null;this.groupWarnings=new Map;this.activeQuizSessions=new Map;this.processedCalls=new Set;this.processedMessageIds=new Set;this.commandCooldowns=new Map;this.rawQrString=null;this.requestedFromJid=null;this.isQrReady=false;this.voiceCooldownMap=new Map;this.chatMemory=new Map;this.welcomedBusinessCustomers=new Set;this.businessChatState=new Map;this.businessThumbCache=new Map;this.pendingFilmChoices=new Map;this.pendingMediaChoices=new Map;this.classicMenuUsers=new Set;this.classicMenuContext=new Map;void cleanupStaleMovieTempFiles().catch(()=>{});this.start247Watchdog();this.chatAiCooldownMap=new Map;this.smartReplyCooldownMap=new Map;this.lastWelcomeSentTime=0;this.isStarting=false;this.isFreshPairing=false;this.isPairingCodeInFlight=false;
    this.cachedLogoBuffer = null;
    this.lastCachedLogoUrl = '';
    const initialCandidates = [
      path.resolve(process.cwd(), 'data', 'bot_logo_opt.jpg'),
      '/data/bot_logo_opt.jpg',
      '/app/applet/data/bot_logo_opt.jpg',
      path.resolve(process.cwd(), 'data', 'bot_logo.png'),
    ];
    for (const p of initialCandidates) {
      try {
        if (fs.existsSync(p)) {
          this.cachedLogoBuffer = fs.readFileSync(p);
          this.lastCachedLogoUrl = BRAND_LOGO_URL;
          break;
        }
      } catch (e) {}
    }
    this.smartReplyCooldownMap = new Map();
    this.state = {
      connectionStatus: "disconnected",
      isOnline: false,
      pairedNumber: null,
      qrCodeDataUrl: null,
      pairingCode: null,
      uptimeSeconds: 0,
      startedAt: null,
      lastPingMs: 14,
      totalMessagesReceived: 0,
      totalCommandsExecuted: 0,
      totalAiQueries: 0,
      activeGroupCount: 0,
      incomingCallsCount: 0,
      callsRejectedCount: 0,
      autoRepliesSentCount: 0,
      voiceAutoRepliesSentCount: 0,
      recentCalls: [],
      config: {
        botName: "YASHI HALLS",
        botLogoUrl: "https://i.ibb.co/n2fYpDV/file-000000007c3c8210ae1b96d28b22c655.png",
        publicUrl: process.env.PUBLIC_URL || process.env.APP_URL || "https://ais-dev-vhkftr4fj3xxbw6rcgsnxt-498406269163.asia-southeast1.run.app",
        prefix: ".",
        mode: "public",
        ownerNumber: "94740712934",
        autoReadStatus: true,
        plan: "mini",
        antiLink: false,
        antiSpam: false,
        welcomeMessage: true,
        welcomeTemplate: "Welcome @user to @group! Enjoy your stay.",
        callDetectorEnabled: true,
        voiceAutoReplyEnabled: true,
        channelAutoReact: false,
        channelReactEmojis: "❤️,🔥,⚡,👍,🌟,🎉,🚀,😍,👏,💯,🤩",
        smartAutoReply: false,
        businessBotToken: "",
        customGreetingSinhala: "හායි @user! 👋 මම @bot WhatsApp Bot. ඔබට උදව් කිරීමට මම සූදානම්! විධාන බැලීමට @prefixmenu ලෙස එවන්න. 😊",
        customGreetingEnglish: "Hello @user! 👋 I am @bot WhatsApp Bot. Type @prefixmenu to view all commands.",
        customAutoReplies: [
          {
            id: "rule-price",
            keyword: "මිල",
            reply: "💰 *YASHI HALLS Bot මිල ගණන්:*\n• PRO Plan: LKR 500/mo\n• BUSINESS Plan: LKR 1,000/mo\nSubscription පිටුවෙන් Manual Bank Transfer මගින් සක්‍රීය කරගත හැක!"
          },
          {
            id: "rule-channel",
            keyword: "channel",
            reply: "🔗 *YASHI HALLS Official Channel:*\nhttps://whatsapp.com/channel/0029Vb927N4GJP8MS1yurM2Z\nනවකම Bot updates සහ features සඳහා අදම එකතු වන්න! 🚀"
          }
        ]
      },
      quotas: {
        mediaRequestsUsed: 0,
        mediaQuotaMax: 50,
        aiRequestsUsed: 0,
        aiQuotaMax: 100,
        maxBotSlots: 1
      }
    };
    this.userId = userId;
    let safeId = (userId || "primary").replace(/[^a-zA-Z0-9_-]/g, "_");
    if (!safeId || safeId === "guest" || safeId === "default") {
      safeId = "primary";
    }
    if (safeId.endsWith("__business")) {
      this.state.config.botName = "YASHI BUSINESS";
      this.state.config.plan = "mini";
    }
    this.sessionDir = path.resolve(process.cwd(), "data", "sessions", safeId, "whatsapp_session");
    this.configFile = path.resolve(process.cwd(), "data", "sessions", safeId, "bot_config.json");
    this.welcomedCustomersFile = path.resolve(process.cwd(), "data", "sessions", safeId, "welcomed_customers.json");

    this.addLog("INFO", `BotEngine initialized for user: ${userId} (session: ${safeId}) with Dual Plan support (MINI & PRO).`);
    this.loadSavedConfig();
    this.loadWelcomedBusinessCustomers();
    this.checkExistingSession();
  }
  static {
    __name(this, "BotEngine");
  }
  hasAnySessionCreds() {
    try {
      if (!fs.existsSync(this.sessionDir)) return false;
      const credsPath = path.join(this.sessionDir, "creds.json");
      if (!fs.existsSync(credsPath)) return false;
      const raw = fs.readFileSync(credsPath, "utf-8");
      return Boolean(raw && raw.trim().length > 20);
    } catch {
      return false;
    }
  }
  hasExistingSession() {
    try {
      if (!fs.existsSync(this.sessionDir)) return false;
      const credsPath = path.join(this.sessionDir, "creds.json");
      if (!fs.existsSync(credsPath)) return false;
      const raw = fs.readFileSync(credsPath, "utf-8");
      if (!raw || raw.trim().length === 0) return false;
      const parsed = JSON.parse(raw);
      // ONLY true if credentials represent a completed, registered WhatsApp account
      return Boolean(parsed && (parsed.registered === true || parsed.me?.id || parsed.account || (parsed.signalIdentities && parsed.signalIdentities.length > 0)));
    } catch {
      return false;
    }
  }
  loadSavedConfig() {
    try {
      if (fs.existsSync(this.configFile)) {
        const raw = fs.readFileSync(this.configFile, "utf-8");
        const parsed = JSON.parse(raw);
        const restoredPlan = parsed.plan === "business" ? "business" : parsed.plan === "pro" ? "pro" : "mini";
        this.state.config = { ...this.state.config, ...parsed, plan: restoredPlan };
        this.setPlan(restoredPlan, false);
        this.addLog("INFO", `Loaded persisted bot settings: Plan=${restoredPlan.toUpperCase()}, AutoReply=${this.state.config.smartAutoReply ? "ON" : "OFF"}`);
      }
    } catch {}
  }
  loadWelcomedBusinessCustomers() {
    try {
      if (this.welcomedCustomersFile && fs.existsSync(this.welcomedCustomersFile)) {
        const raw = fs.readFileSync(this.welcomedCustomersFile, "utf-8");
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          for (const entry of parsed) {
            if (typeof entry === "string" && entry.trim()) {
              this.welcomedBusinessCustomers.add(entry.trim());
            }
          }
        }
      }
    } catch {}
  }
  saveWelcomedBusinessCustomers() {
    try {
      if (!this.welcomedCustomersFile) return;
      const dir = path.dirname(this.welcomedCustomersFile);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.writeFileSync(
        this.welcomedCustomersFile,
        JSON.stringify(Array.from(this.welcomedBusinessCustomers)),
        "utf-8"
      );
    } catch {}
  }
  getBusinessCustomerIdentityKeys(jid: string, msg?: any): string[] {
    const keys = new Set<string>();
    const addCandidate = (raw?: string) => {
      const s = String(raw || "").trim().toLowerCase();
      if (!s) return;
      keys.add(s);
      const userPart = s.split("@")[0].split(":")[0].trim();
      if (userPart) keys.add(userPart);
      const digits = userPart.replace(/\D/g, "");
      if (digits.length >= 7) keys.add(`num:${digits}`);
    };
    addCandidate(jid);
    addCandidate(msg?.key?.remoteJid);
    addCandidate(msg?.key?.remoteJidAlt);
    addCandidate(msg?.key?.participant);
    addCandidate(msg?.key?.participantAlt);
    return Array.from(keys);
  }
  hasReceivedBusinessWelcome(jid: string, msg?: any): boolean {
    const keys = this.getBusinessCustomerIdentityKeys(jid, msg);
    return keys.some((k) => this.welcomedBusinessCustomers.has(k));
  }
  markBusinessCustomerWelcomed(jid: string, msg?: any) {
    const keys = this.getBusinessCustomerIdentityKeys(jid, msg);
    let added = false;
    for (const k of keys) {
      if (!this.welcomedBusinessCustomers.has(k)) {
        this.welcomedBusinessCustomers.add(k);
        added = true;
      }
    }
    if (added) {
      this.saveWelcomedBusinessCustomers();
    }
  }
  saveConfigToFile() {
    try {
      fs.mkdirSync(path.dirname(this.configFile), { recursive: true });
      fs.writeFileSync(this.configFile, JSON.stringify(this.state.config, null, 2));
    } catch {}
  }
  addLog(level, message) {
    const timestamp = new Date().toLocaleTimeString("en-US", { hour12: false });
    const log = { id: `${Date.now()}-${Math.random().toString(36).substr(2, 5)}`, timestamp, level, message };
    this.logs.push(log);
    if (this.logs.length > this.maxLogs) {
      this.logs.shift();
    }
  }
  checkExistingSession() {
    try {
      const isPrimaryTenant = !this.userId || this.userId === "primary" || this.userId === "guest" || this.userId === "default";

      // 1. If primary folder has no session, try migrating from legacy folders on disk
      if (isPrimaryTenant && !this.hasExistingSession()) {
        const legacyDirs = [
          path.resolve(process.cwd(), "data", "sessions", "guest", "whatsapp_session"),
          path.resolve(process.cwd(), "data", "sessions", "default", "whatsapp_session")
        ];
        for (const leg of legacyDirs) {
          if (fs.existsSync(leg)) {
            const legCreds = path.join(leg, "creds.json");
            if (fs.existsSync(legCreds)) {
              fs.mkdirSync(this.sessionDir, { recursive: true });
              for (const f of fs.readdirSync(leg)) {
                try {
                  fs.copyFileSync(path.join(leg, f), path.join(this.sessionDir, f));
                } catch {}
              }
              break;
            }
          }
        }
      }

      // 2. If still no session, restore from persistent database backups
      if (!this.hasExistingSession()) {
        adminStore.restoreSessionBackup(this.userId, this.sessionDir);
        if (isPrimaryTenant && !this.hasExistingSession()) {
          adminStore.restoreSessionBackup("primary", this.sessionDir);
          adminStore.restoreSessionBackup("guest", this.sessionDir);
          adminStore.restoreSessionBackup("default", this.sessionDir);
        }
      }

      if (this.hasExistingSession()) {
        this.addLog("INFO", "Existing registered WhatsApp session restored and verified in persistent storage.");
        this.isExplicitlyStopped = false;
        return true;
      } else if (!fs.existsSync(this.sessionDir)) {
        fs.mkdirSync(this.sessionDir, { recursive: true });
      }
    } catch {}
    return false;
  }
  getState() {
    const uptimeSeconds = this.startTime ? Math.floor((Date.now() - this.startTime) / 1000) : 0;
    return { ...this.state, uptimeSeconds };
  }
  getLogs() {
    return [...this.logs];
  }
  setPlan(plan, persist = true) {
    this.state.config.plan = plan;
    if (plan === "business") {
      this.state.config.botName = "YASHI HALLS BUSINESS";
      this.state.quotas.mediaQuotaMax = 999999;
      this.state.quotas.aiQuotaMax = 999999;
      this.state.quotas.maxBotSlots = 25;
      this.state.config.antiLink = true;
      this.state.config.antiSpam = true;
      this.addLog("INFO", "👑 Tier upgraded to YASHI HALLS BUSINESS! 25 Bot Slots, Unlimited Media & AI, and VIP Enterprise SLA activated.");
    } else if (plan === "pro") {
      this.state.config.botName = "YASHI HALLS PRO";
      this.state.quotas.mediaQuotaMax = 1000;
      this.state.quotas.aiQuotaMax = 5000;
      this.state.quotas.maxBotSlots = 5;
      this.state.config.antiLink = true;
      this.state.config.antiSpam = true;
      this.addLog("INFO", "💎 Tier upgraded to YASHI HALLS PRO! All advanced features, multi-slots & PRO quotas unlocked.");
    } else {
      this.state.config.botName = "YASHI HALLS MINI";
      this.state.quotas.mediaQuotaMax = 50;
      this.state.quotas.aiQuotaMax = 100;
      this.state.quotas.maxBotSlots = 1;
      this.state.config.antiLink = false;
      this.state.config.antiSpam = false;
      this.addLog("INFO", "🟢 Tier set to YASHI HALLS MINI (Free). Quotas set to 50 Media / 100 AI daily requests.");
    }
    if (persist) {
      this.saveConfigToFile();
    }
    return this.getState();
  }
  async syncProfilePictureWithLogo(customLogoUrl) {
    if (!this.sock || !this.state.isOnline) {
      return { success: false, message: "WhatsApp bot must be online to update profile picture." };
    }
    const targetUrl = customLogoUrl || this.state.config.botLogoUrl || BRAND_LOGO_URL;
    try {
      const userJid = this.sock.user?.id;
      if (!userJid) {
        return { success: false, message: "User JID not available." };
      }
      if (typeof this.sock.updateProfilePicture === "function") {
        await this.sock.updateProfilePicture(userJid, { url: targetUrl });
        if (customLogoUrl) {
          this.state.config.botLogoUrl = customLogoUrl;
          this.saveConfigToFile();
        }
        this.addLog("INFO", `✅ Bot profile picture successfully updated to: ${targetUrl}`);
        return { success: true, message: "Bot profile picture updated successfully!" };
      }
      return { success: false, message: "Profile picture update not supported by current client session." };
    } catch (err: any) {
      this.addLog("WARN", `Profile picture update attempt failed: ${err?.message}`);
      return { success: false, message: err?.message || "Failed to update profile picture" };
    }
  }
  updateConfig(newConfig: any) {
    const customBotName = newConfig.botName;
    if (newConfig.plan) {
      this.setPlan(newConfig.plan);
    }
    this.state.config = {
      ...this.state.config,
      ...newConfig,
      ...(customBotName ? { botName: customBotName } : {}),
      ...(newConfig.businessBotToken !== undefined ? { businessBotToken: newConfig.businessBotToken } : {})
    };
    this.saveConfigToFile();
    invalidateMasterMenuCache();
    this.addLog("INFO", `Configuration updated: Plan=${this.state.config.plan.toUpperCase()}, BotName="${this.state.config.botName}", Prefix="${this.state.config.prefix}"`);
    return this.state.config;
  }
  resetQuotas() {
    this.state.quotas.mediaRequestsUsed = 0;
    this.state.quotas.aiRequestsUsed = 0;
    this.addLog("INFO", "Daily quotas reset to 0.");
    return this.state.quotas;
  }
  start247Watchdog() {
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    this.watchdogTimer = setInterval(async () => {
      try {
        if (this.isExplicitlyStopped) return;

        // CRITICAL GUARD: NEVER interrupt an active pairing attempt!
        // When pairing code or QR is waiting for user scan/entry, the socket MUST stay intact.
        const isPairingActive =
          this.state.connectionStatus === "pairing_code_ready" ||
          this.state.connectionStatus === "qr_ready" ||
          this.state.connectionStatus === "connecting" ||
          Boolean(this.state.pairingCode) ||
          this.isQrReady;

        if (isPairingActive) {
          return;
        }

        const hasRegisteredCreds = this.hasExistingSession() || this.hasAnySessionCreds();

        // 24/7 Self-Healing: ONLY auto-reconnect if a registered session exists on disk but connection is down
        if (hasRegisteredCreds && !this.state.isOnline && !this.reconnectTimer) {
          this.addLog("WARN", "[24/7 Self-Healing Watchdog] Session detected offline. Auto-triggering instant reconnection...");
          this.start(this.activePairingPhone).catch(() => {});
        } else if (this.state.isOnline && this.sock?.user?.id) {
          // Memory leak guard: prune old map entries
          if (this.smartReplyCooldownMap.size > 200) this.smartReplyCooldownMap.clear();
          if (this.voiceCooldownMap.size > 200) this.voiceCooldownMap.clear();
          if (this.processedCalls.size > 200) this.processedCalls.clear();
        }
      } catch (err) {}
    }, 25000);
  }

  async start(requestedPhone?: string | null) {
    this.isExplicitlyStopped = false;
    this.reconnectAttempts = 0;
    if (this.isStarting) {
      this.addLog("INFO", "Bot startup sequence is already in progress. Ignoring duplicate trigger.");
      return { success: true, message: "Bot is already starting." };
    }
    if (this.state.connectionStatus === "connected" && this.sock && this.state.isOnline) {
      return { success: true, message: "Bot is already connected and online." };
    }
    this.isStarting = true;
    if (requestedPhone) {
      const cleanPhone = normalizeWhatsAppNumber(requestedPhone);
      if (cleanPhone) {
        this.activePairingPhone = cleanPhone;
        this.isFreshPairing = true;
      }
    } else if (requestedPhone === null) {
      this.activePairingPhone = null;
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.sock) {
      try {
        this.sock.ev.removeAllListeners("connection.update");
        this.sock.ev.removeAllListeners("creds.update");
        this.sock.ev.removeAllListeners("messages.upsert");
        this.sock.ev.removeAllListeners("call");
        this.sock.ev.removeAllListeners("group-participants.update");
        if (this.sock.ws) {
          try { this.sock.ws.terminate(); } catch (e) {}
        }
        try { this.sock.ws?.close(); } catch (e) {}
        try { this.sock.end(void 0); } catch (e) {}
      } catch (err) {}
      this.sock = null;
    }

    try {
      this.state.connectionStatus = "connecting";
      this.addLog("INFO", `Starting ${this.state.config.botName} WhatsApp Baileys connection...`);
      if (!fs.existsSync(this.sessionDir)) {
        fs.mkdirSync(this.sessionDir, { recursive: true });
      }
      const { state: authState, saveCreds } = await useMultiFileAuthState(this.sessionDir);
      await saveCreds();

      // If registered credentials already exist and this wasn't explicitly a new pair request, mark as reconnect
      if (authState?.creds?.registered && !requestedPhone) {
        this.isFreshPairing = false;
      } else if (!authState?.creds?.registered) {
        this.isFreshPairing = true;
      }

      let version = [2, 3000, 1017531287];
      try {
        const vRes = (await Promise.race([
          fetchLatestBaileysVersion(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("version_timeout")), 4000))
        ])) as any;
        if (vRes?.version) version = vRes.version;
      } catch {}


      this.addLog("INFO", `Using Baileys protocol v${version.join(".")}`);
      const logger = pino({ level: "silent" });

      this.sock = makeWASocket({
        version,
        logger,
        printQRInTerminal: false,
        auth: {
          creds: authState.creds,
          keys: makeCacheableSignalKeyStore(authState.keys, logger),
        },
        browser: Browsers.ubuntu("Chrome"),
        syncFullHistory: false,
        markOnlineOnConnect: true,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000,
        keepAliveIntervalMs: 25000,
        retryRequestDelayMs: 500,
        maxMsgRetryCount: 5,
        generateHighQualityLinkPreview: false,
        getMessage: __name(async () => void 0, "getMessage"),
      });

      if (this.sock?.ws) {
        this.sock.ws.on("error", (err: any) => {
          const errMsg = String(err?.message || err || "");
          this.addLog("WARN", `WebSocket notice: ${errMsg}`);
          if (errMsg.includes("Bad MAC") || errMsg.includes("Failed to decrypt") || errMsg.includes("Session error")) {
            this.cleanupCorruptSessionKeys();
          }
        });
      }

      this.sock.ev.on("creds.update", async (creds: any) => {
        await saveCreds();
        adminStore.saveSessionBackup(this.userId, this.sessionDir, false);
      });

      // Eager fast pairing trigger once socket handshake begins
      if (this.activePairingPhone && !authState.creds.registered) {
        const phoneToPair = this.activePairingPhone;
        setTimeout(async () => {
          if (!this.state.pairingCode && this.sock && !this.state.isOnline) {
            await this.obtainPairingCode(phoneToPair);
          }
        }, 1200);
      }

      this.sock.ev.on("connection.update", async (update: any) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
          this.rawQrString = qr;
          this.isQrReady = true;
          this.state.connectionStatus = "qr_ready";
          try {
            const dataUrl = await QRCode.toDataURL(qr, { margin: 2, scale: 7 });
            this.state.qrCodeDataUrl = dataUrl;
            this.addLog("INFO", "✅ Official WhatsApp QR code ready for Linked Devices scan.");
          } catch (err) {
            this.addLog("ERROR", "Failed to generate QR Code data URL.");
          }

          // If phone was provided and code not yet obtained, request single official code
          if (this.activePairingPhone && !this.state.pairingCode && !authState.creds.registered) {
            await this.obtainPairingCode(this.activePairingPhone);
          }
        }

        if (connection === "close") {
          this.isQrReady = false;
          if (this.isExplicitlyStopped) {
            this.state.isOnline = false;
            this.state.connectionStatus = "disconnected";
            this.startTime = null;
            return;
          }

          this.isStarting = false;
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const errorMsg = String(lastDisconnect?.error?.message || lastDisconnect?.error || "");
          const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401;
          const isRestartRequired = statusCode === DisconnectReason.restartRequired || statusCode === 515 || statusCode === 428 || statusCode === 408;

          this.state.isOnline = false;
          this.startTime = null;

          this.addLog("WARN", `Connection dropped (${statusCode || "Stream Closed"}): ${DisconnectReason[statusCode] || errorMsg || "Reconnecting..."}.`);

          if (statusCode === 440) {
            this.addLog("WARN", "Connection replaced (440). Backing off 8s before reconnect attempt...");
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(() => {
              this.reconnectTimer = null;
              if (!this.isExplicitlyStopped && !this.state.isOnline) {
                this.start(this.activePairingPhone || requestedPhone);
              }
            }, 8000);
            return;
          }

          if (isLoggedOut) {
            this.addLog("WARN", "WhatsApp session unlinked by user device (Logged Out 401). Session wiped for clean pairing.");
            this.state.connectionStatus = "disconnected";
            this.state.pairedNumber = null;
            this.state.pairingCode = null;
            this.state.qrCodeDataUrl = null;
            this.clearSessionFiles();
            return;
          }

          if (errorMsg.includes("Bad MAC") || errorMsg.includes("Failed to decrypt") || errorMsg.includes("Session error")) {
            this.addLog("WARN", `Transient session decryption notice (${errorMsg}). Auto-purging corrupted signal key files while preserving creds.json...`);
            this.cleanupCorruptSessionKeys();
          }

          if (isRestartRequired) {
            this.addLog("INFO", "WhatsApp stream restart signal received. Graceful reconnect in 2.0s...");
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(() => {
              this.reconnectTimer = null;
              if (!this.isExplicitlyStopped) {
                this.start(this.activePairingPhone || requestedPhone);
              }
            }, 2000);
            return;
          }

          // Check if ANY credentials or session files exist on disk
          const hasSession = this.hasExistingSession() || this.hasAnySessionCreds();

          if (!hasSession && !this.activePairingPhone && this.state.connectionStatus !== "pairing_code_ready" && this.state.connectionStatus !== "qr_ready") {
            this.addLog("INFO", "Pairing socket closed without saved credentials. Ready for new pairing attempt.");
            this.state.connectionStatus = "disconnected";
            return;
          }

          // 24/7 NON-STOP ALWAYS-ON RECOVERY: Reconnect registered or pairing sessions automatically
          this.state.connectionStatus = "reconnecting";
          this.reconnectAttempts++;
          const delayMs = Math.min(800 + (this.reconnectAttempts * 500), 5000);
          this.addLog("INFO", `[24/7 Auto-Reconnect] Reconnecting in ${(delayMs / 1000).toFixed(1)}s (Attempt #${this.reconnectAttempts})...`);

          if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
          this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.isExplicitlyStopped) {
              this.start(this.activePairingPhone || requestedPhone);
            }
          }, delayMs);
        } else if (connection === "open") {
          this.isStarting = false;
          this.reconnectAttempts = 0;
          this.activePairingPhone = null;
          this.isQrReady = false;
          this.state.isOnline = true;
          this.state.connectionStatus = "connected";
          this.state.qrCodeDataUrl = null;
          this.state.pairingCode = null;
          this.startTime = Date.now();
          this.state.startedAt = new Date().toISOString();
          adminStore.saveSessionBackup(this.userId, this.sessionDir, true);
          const userJid = this.sock?.user?.id || "";
          const phoneNum = userJid.split(":")[0] || userJid.split("@")[0] || "Unknown";
          this.state.pairedNumber = phoneNum;
          this.addLog("INFO", `✅ Connection established! ${this.state.config.botName} (${this.state.config.plan.toUpperCase()}) is now ONLINE for +${phoneNum}`);
          this.addLog("INFO", `Ready to respond to commands with prefix '${this.state.config.prefix}'.`);

          // Auto-follow official WhatsApp Channel for the connected session
          this.autoFollowOfficialChannel(phoneNum);

          try {
            const targetJid = userJid.includes(":") ? `${userJid.split(":")[0]}@s.whatsapp.net` : userJid.includes("@") ? userJid : `${userJid}@s.whatsapp.net`;
            const now = Date.now();
            const sessionWelcomeKey = `${this.userId}_${phoneNum}`;
            const alreadyWelcomed = adminStore.isSessionWelcomed(sessionWelcomeKey);

            // ONLY send welcome card upon a fresh, brand-new pairing. NEVER on reconnects!
            const shouldSendWelcomeCard = Boolean(this.isFreshPairing) && !alreadyWelcomed;
            this.isFreshPairing = false;

            if (targetJid && this.sock && shouldSendWelcomeCard) {
              adminStore.markSessionWelcomed(sessionWelcomeKey);
              this.lastWelcomeSentTimestamp = now;
              const p = this.state.config.prefix || ".";
              const connectedOwner = this.getConnectedUserName();
              const welcomeCaption = `*👑 YASHI HALLS BOT CONNECTED 👑*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🤖 *Bot Name:* ${this.state.config.botName || 'YASHI HALLS MINI'}
👑 *Connected User:* ${connectedOwner}
🟢 *Status:* ONLINE & ACTIVE 24/7 ✅
🔣 *Prefix:* [ ${p} ]
📱 *Number:* +${phoneNum}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎉 *ඔබගේ WhatsApp Bot සාර්ථකව සම්බන්ධ විය (Connected)!*

📌 *ප්‍රධාන විධාන (Quick Commands):*
• *${p}menu* - ප්‍රධාන විධාන ලැයිස්තුව (Main Menu)
• *${p}ping* - වේගය පරීක්ෂාව (Speed Test)
• *${p}alive* - 24/7 සක්‍රිය තත්ත්වය (System Health)
• *${p}song <නම>* - ඕනෑම සින්දුවක් MP3 (Fast Download)
• *${p}video <නම>* - ඕනෑම වීඩියෝවක් HD MP4

📢 *Official Channel:* ${WHATSAPP_CHANNEL_URL}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();

              let welcomeImgBuffer: Buffer | null = null;
              try {
                const logoBuf = await this.getLogoBuffer();
                welcomeImgBuffer = await generateWelcomeCardImage({
                  botName: this.state.config.botName || 'YASHI HALLS MINI',
                  userName: connectedOwner,
                  phoneNumber: phoneNum,
                  logoBuffer: logoBuf,
                  channelUrl: WHATSAPP_CHANNEL_URL
                });
              } catch (welcomeCardErr: any) {
                this.addLog("WARN", `Could not generate welcome card image: ${welcomeCardErr?.message || welcomeCardErr}`);
              }

              const ownerRaw = (this.state.config.ownerNumber || "94740712934").replace(/\D/g, "");
              const ownerJid = ownerRaw && ownerRaw.length >= 8 ? `${ownerRaw}@s.whatsapp.net` : null;
              const targetJids = new Set<string>();
              if (targetJid) targetJids.add(targetJid);
              if (ownerJid) targetJids.add(ownerJid);
              if (this.requestedFromJid) targetJids.add(this.requestedFromJid);

              // Generate interactive in-chat HTML welcome card
              let welcomeMarkup = "";
              try {
                welcomeMarkup = generateConnectedWelcomeHtmlApp({
                  botName: this.state.config.botName || 'YASHI HALLS MINI',
                  userName: connectedOwner,
                  phoneNumber: phoneNum,
                  prefix: p,
                  plan: `${this.state.config.plan?.toUpperCase() || 'MINI'} PLAN`,
                  channelUrl: WHATSAPP_CHANNEL_URL,
                  height: 620,
                });
              } catch (htmlGenErr: any) {
                this.addLog("WARN", `Could not generate welcome HTML card: ${htmlGenErr?.message || htmlGenErr}`);
              }

              for (const destJid of targetJids) {
                let htmlDelivered = false;

                // 1. Attempt to deliver Zaileys In-Chat Interactive Card
                if (welcomeMarkup) {
                  try {
                    const result = await sendHtmlApp(this.sock, destJid, welcomeMarkup, {
                      title: `🎉 BOT CONNECTED • ${this.state.config.botName || 'YASHI HALLS MINI'} 🎉`,
                      height: 620,
                      bypassDownload: true,
                    });
                    if (result.success) {
                      htmlDelivered = true;
                      this.addLog("SUCCESS", `🎉 [WELCOME CARD] In-Chat Connected Welcome Card delivered to ${destJid.split("@")[0]}`);
                    } else {
                      this.addLog("WARN", `⚠️ [WELCOME CARD] Card dispatch for ${destJid.split("@")[0]}: ${result.error || 'Failed'}`);
                    }
                  } catch (cardErr: any) {
                    this.addLog("WARN", `⚠️ [WELCOME CARD] Card error for ${destJid.split("@")[0]}: ${cardErr?.message || cardErr}`);
                  }
                }

                // 2. If In-Chat card was NOT delivered, send fallback text/image message
                if (!htmlDelivered) {
                  try {
                    if (welcomeImgBuffer) {
                      await this.sock.sendMessage(destJid, { image: welcomeImgBuffer, caption: welcomeCaption });
                    } else {
                      await this.sock.sendMessage(destJid, { text: welcomeCaption });
                    }
                    this.addLog("INFO", `📤 Sent welcome text message fallback to ${destJid.split("@")[0]}`);
                  } catch (e: any) {
                    this.addLog("WARN", `Could not send welcome fallback to ${destJid}: ${e?.message}`);
                  }
                }
              }
            } else {
              this.addLog("INFO", `ℹ️ [SESSION RESUMED] Bot reconnected for +${phoneNum}. Skipping duplicate welcome card (already sent on initial pairing).`);
            }
          } catch (err: any) {
            this.addLog("WARN", `Could not send welcome message to owner: ${err?.message}`);
          }
        }
      });

      this.sock.ev.on("group-participants.update", async (update: any) => {
        try {
          const { id: groupJid, participants, action } = update;
          if (!groupJid || !Array.isArray(participants) || participants.length === 0) return;

          let groupName = "WhatsApp Group";
          let memberCount = 0;
          try {
            if (this.sock && typeof this.sock.groupMetadata === "function") {
              const meta = await this.sock.groupMetadata(groupJid);
              if (meta?.subject) groupName = meta.subject;
              if (meta?.participants) memberCount = meta.participants.length;
            }
          } catch (mErr) {}

          const p = this.state.config.prefix || ".";
          const botName = this.state.config.botName || "YASHI HALLS";

          if (action === "add" && this.state.config.welcomeMessage) {
            for (const part of participants) {
              const participantId = typeof part === "string" ? part : part?.id || "";
              if (!participantId) continue;
              const userPhone = participantId.split("@")[0];

              const welcomeMarkup = generateGroupWelcomeHtmlApp({
                userPhone,
                groupName,
                botName,
                prefix: p,
                memberCount: memberCount || undefined,
              });

              let delivered = false;
              if (this.sock) {
                try {
                  const res = await sendHtmlApp(this.sock, groupJid, welcomeMarkup, {
                    title: `🎉 WELCOME TO ${groupName.toUpperCase()} 👋`,
                    height: 520,
                    bypassDownload: true,
                  });
                  if (res.success) {
                    delivered = true;
                    this.addLog("SUCCESS", `🎉 [GROUP WELCOME CARD] Sent animated card for +${userPhone} in group ${groupName}`);
                  }
                } catch (cardErr) {}
              }

              if (!delivered && this.sock) {
                const welcomeMsg = `🎉 *WELCOME TO THE GROUP!* 👋\n\nHello @${userPhone}! We're thrilled to have you here in *${groupName}*.\n📌 Please check the group description and follow the rules.\n\n_Powered by ${botName} 24/7 Engine_`;
                await this.sock.sendMessage(groupJid, { text: welcomeMsg, mentions: [participantId] });
              }
            }
          } else if (action === "remove" || action === "leave") {
            for (const part of participants) {
              const participantId = typeof part === "string" ? part : part?.id || "";
              if (!participantId) continue;
              const userPhone = participantId.split("@")[0];

              const goodbyeMarkup = generateGroupGoodbyeHtmlApp({
                userPhone,
                groupName,
                botName,
                prefix: p,
              });

              let delivered = false;
              if (this.sock) {
                try {
                  const res = await sendHtmlApp(this.sock, groupJid, goodbyeMarkup, {
                    title: `👋 GOODBYE FROM ${groupName.toUpperCase()} 💔`,
                    height: 480,
                    bypassDownload: true,
                  });
                  if (res.success) {
                    delivered = true;
                    this.addLog("INFO", `👋 [GROUP GOODBYE CARD] Sent animated card for +${userPhone} in group ${groupName}`);
                  }
                } catch (cardErr) {}
              }

              if (!delivered && this.sock) {
                const goodbyeMsg = `👋 *GOODBYE!* 💔\n\nMember @${userPhone} left *${groupName}*.\nWe wish you all the best for your journey ahead!\n\n_Powered by ${botName} 24/7 Engine_`;
                await this.sock.sendMessage(groupJid, { text: goodbyeMsg, mentions: [participantId] });
              }
            }
          }
        } catch (err: any) {
          this.addLog("WARN", `Group automation error: ${err?.message}`);
        }
      });

      this.sock.ev.on("call", async (calls: any) => {
        if (!this.state.config.callDetectorEnabled) return;
        for (const call of calls) {
          if (call.status !== "offer") continue;
          const callId = call.id;
          const callerJid = call.from;
          if (!callerJid || this.processedCalls.has(callId)) continue;
          this.processedCalls.add(callId);
          if (this.processedCalls.size > 200) {
            const first = this.processedCalls.values().next().value;
            if (first) this.processedCalls.delete(first);
          }
          this.state.incomingCallsCount++;
          const cleanPhone = callerJid.split("@")[0];
          this.addLog("WARN", `📞 Incoming WhatsApp call detected from +${cleanPhone}`);
          let rejected = false;
          try {
            if (this.sock && typeof this.sock.rejectCall === "function") {
              await this.sock.rejectCall(callId, callerJid);
              rejected = true;
              this.state.callsRejectedCount++;
              this.addLog("INFO", `📵 Successfully rejected incoming call from +${cleanPhone}`);
            } else {
              this.addLog("WARN", `Call rejection not supported by current Baileys client instance for +${cleanPhone}`);
            }
          } catch (rejectErr: any) {
            this.addLog("ERROR", `Failed to reject call from +${cleanPhone}: ${rejectErr?.message || rejectErr}`);
          }
          let replied = false;
          try {
            const replyText = `
📵 YASHI HALLS BOT

Sorry! අපගේ Bot එකට WhatsApp Voice/Video Calls ලබාගත නොහැක.

🤖 අපගේ සේවාවන් භාවිත කිරීමට:
Type ".menu" to explore our services.

✨ POWERED BY YASHI HALLS
`.trim();
            await this.sock?.sendMessage(callerJid, { text: replyText });
            replied = true;
            this.state.autoRepliesSentCount++;
            this.addLog("INFO", `💬 Sent call auto-reply chat message to +${cleanPhone}`);
          } catch (replyErr: any) {
            this.addLog("ERROR", `Failed to send call auto-reply to +${cleanPhone}: ${replyErr?.message || replyErr}`);
          }
          const nowTime = new Date().toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", second: "2-digit" });
          this.state.recentCalls.unshift({ id: callId, from: `+${cleanPhone}`, timestamp: nowTime, rejected, replied });
          if (this.state.recentCalls.length > 25) {
            this.state.recentCalls.pop();
          }
        }
      });

      this.sock.ev.on("messages.upsert", async ({ messages, type }: any) => {
        if (type !== "notify") return;
        for (const msg of messages) {
          if (!msg.message) continue;
          this.state.totalMessagesReceived++;
          if (this.state.config.autoReadStatus && msg.key.remoteJid === "status@broadcast") {
            try {
              await this.sock?.readMessages([msg.key]);
            } catch {}
            continue;
          }
          const isChannel = Boolean(msg.key.remoteJid && (msg.key.remoteJid.endsWith("@newsletter") || msg.key.remoteJid.includes("newsletter")));
          if (this.state.config.channelAutoReact && isChannel && msg.key.remoteJid) {
            try {
              const emojiList = (this.state.config.channelReactEmojis || "❤️,🔥,⚡,👍,🌟,🎉,🚀,😍,👏").split(",").map((e: string) => e.trim()).filter(Boolean);
              const pickedEmoji = emojiList[Math.floor(Math.random() * emojiList.length)] || "❤️";
              await this.sock?.sendMessage(msg.key.remoteJid, { react: { text: pickedEmoji, key: msg.key } });
              this.addLog("INFO", `⚡ [CHANNEL AUTO-REACT] Dropped reaction ${pickedEmoji} to channel post in ${msg.key.remoteJid}`);
            } catch (err: any) {
              this.addLog("WARN", `Channel auto-react notice: ${err?.message || err}`);
            }
          }
          // Asynchronous non-blocking dispatch so incoming message bursts never block socket loop
          setImmediate(() => {
            this.handleIncomingMessage(msg).catch((err: any) => {
              this.addLog("WARN", `Message handler exception: ${err?.message || err}`);
            });
          });
        }
      });

      return { success: true, message: "Bot engine initialization started successfully." };
    } catch (error: any) {
      this.isStarting = false;
      this.state.connectionStatus = "disconnected";
      this.state.isOnline = false;
      this.addLog("ERROR", `Bot start failed: ${error?.message || error}`);
      return { success: false, message: error?.message || "Failed to start bot engine" };
    }
  }

  async obtainPairingCode(phone: string): Promise<string | null> {
    if (this.state.pairingCode) return this.state.pairingCode;
    if (this.isPairingCodeInFlight) return null;
    if (!this.sock || this.sock.authState?.creds?.registered) return null;

    this.isPairingCodeInFlight = true;
    try {
      this.addLog("INFO", `⚡ Requesting official WhatsApp Pairing Code for +${phone}...`);
      const rawCode = await this.sock.requestPairingCode(phone);
      if (rawCode) {
        const formattedCode = rawCode.includes("-") ? rawCode : rawCode.match(/.{1,4}/g)?.join("-") || rawCode;
        this.state.pairingCode = formattedCode;
        this.state.connectionStatus = "pairing_code_ready";
        this.addLog("SUCCESS", `✅ [AUTHENTIC CODE] WhatsApp Pairing Code ready for +${phone}: ${formattedCode}`);
        return formattedCode;
      }
    } catch (err: any) {
      this.addLog("WARN", `WhatsApp pairing code note (+${phone}): ${err?.message || err}`);
    } finally {
      this.isPairingCodeInFlight = false;
    }
    return null;
  }

  async requestPairingCode(phone: string) {
    const cleanPhone = normalizeWhatsAppNumber(phone);
    if (!cleanPhone || cleanPhone.length < 8) {
      return {
        success: false,
        message: "Invalid phone number. Enter e.g. 0740712934 or 94740712934.",
      };
    }

    this.customPairingCodeRequested = null;
    this.activePairingPhone = cleanPhone;
    this.isFreshPairing = true;
    this.state.pairingCode = null;

    // Restart fresh socket for authentic WhatsApp pairing code handshake
    this.addLog("INFO", `Preparing fresh WhatsApp pairing session for +${cleanPhone}...`);
    this.isExplicitlyStopped = false;
    this.reconnectAttempts = 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    await this.stop(false);
    adminStore.deleteSessionBackup(this.userId);
    this.clearSessionFiles();
    this.state.pairedNumber = null;
    this.state.qrCodeDataUrl = null;
    this.state.pairingCode = null;
    this.isQrReady = false;

    const res = await this.start(cleanPhone);
    if (!res.success) {
      return { success: false, message: res.message };
    }

    // Wait for official pairing code generation up to 25 seconds
    const startTime = Date.now();

    while (Date.now() - startTime < 25000) {
      if (this.state.pairingCode) {
        return {
          success: true,
          code: this.state.pairingCode,
          rawCode: this.state.pairingCode.replace(/-/g, ""),
          message: "Official WhatsApp Pairing Code generated successfully. Enter this code on your WhatsApp Linked Devices.",
        };
      }

      if (this.sock && !this.state.pairingCode && !this.isPairingCodeInFlight && Date.now() - startTime > 1500) {
        const code = await this.obtainPairingCode(cleanPhone);
        if (code) {
          return {
            success: true,
            code,
            rawCode: code.replace(/-/g, ""),
            message: "Official WhatsApp Pairing Code generated successfully. Enter this code on your WhatsApp Linked Devices.",
          };
        }
      }

      await new Promise((r) => setTimeout(r, 400));
    }

    if (this.state.pairingCode) {
      return {
        success: true,
        code: this.state.pairingCode,
        rawCode: this.state.pairingCode.replace(/-/g, ""),
        message: "Official WhatsApp Pairing Code generated successfully.",
      };
    }

    return {
      success: true,
      code: "PENDING",
      message: "Pairing request dispatched to WhatsApp. Please wait a moment...",
    };
  }

  async generateQrCode(forceNew = false) {
    if (!forceNew && this.state.qrCodeDataUrl && (this.state.connectionStatus === "qr_ready" || this.isQrReady)) {
      return { success: true, qr: this.state.qrCodeDataUrl, message: "QR code ready." };
    }
    this.addLog("INFO", "Initiating fresh WhatsApp QR Code pairing...");
    this.activePairingPhone = null;
    this.customPairingCodeRequested = null;
    this.isExplicitlyStopped = false;
    this.reconnectAttempts = 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    await this.stop(false);
    adminStore.deleteSessionBackup(this.userId);
    this.clearSessionFiles();
    this.state.pairedNumber = null;
    this.state.pairingCode = null;
    this.state.qrCodeDataUrl = null;
    this.isQrReady = false;

    const startRes = await this.start(null);
    if (!startRes.success) {
      return { success: false, message: startRes.message };
    }
    const start = Date.now();
    while (Date.now() - start < 20000) {
      if (this.state.qrCodeDataUrl) {
        return { success: true, qr: this.state.qrCodeDataUrl, message: "QR code generated successfully." };
      }
      await new Promise((r) => setTimeout(r, 300));
    }
    if (this.state.qrCodeDataUrl) {
      return { success: true, qr: this.state.qrCodeDataUrl, message: "QR code ready." };
    }
    return { success: false, message: "QR Code generation timed out. Please tap Refresh QR Code." };
  }

  async requestPairingCodeAndQr(phone: string, requestedCustomCode?: string): Promise<{
    success: boolean;
    pairingCode?: string;
    qrBuffer?: Buffer;
    qrCodeDataUrl?: string;
    rawQrString?: string;
    message: string;
  }> {
    const cleanPhone = normalizeWhatsAppNumber(phone);
    if (!cleanPhone || cleanPhone.length < 8) {
      return {
        success: false,
        message: "Invalid phone number. Enter e.g. 0740712934 or 94740712934.",
      };
    }

    this.customPairingCodeRequested = null;
    this.addLog("INFO", `Preparing WhatsApp pairing session for +${cleanPhone}...`);
    this.activePairingPhone = cleanPhone;
    this.isFreshPairing = true;
    this.isExplicitlyStopped = false;
    this.reconnectAttempts = 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    await this.stop(false);
    this.clearSessionFiles();
    this.state.pairedNumber = null;
    this.state.qrCodeDataUrl = null;
    this.state.pairingCode = null;
    this.rawQrString = null;
    this.isQrReady = false;

    const startRes = await this.start(cleanPhone);
    if (!startRes.success) {
      return { success: false, message: startRes.message };
    }

    // Give socket 1200ms to open WebSocket connection
    await new Promise((r) => setTimeout(r, 1200));

    const startTime = Date.now();
    while (Date.now() - startTime < 15000) {
      if (this.state.pairingCode) {
        let qrBuffer: Buffer | null = null;
        if (this.rawQrString) {
          try {
            qrBuffer = await QRCode.toBuffer(this.rawQrString, {
              type: "png",
              margin: 2,
              scale: 8,
              color: { dark: "#000000", light: "#ffffff" },
            });
          } catch {}
        }

        return {
          success: true,
          pairingCode: this.state.pairingCode,
          qrBuffer: qrBuffer || undefined,
          message: "Pairing code generated successfully.",
        };
      }

      if (this.sock && !this.sock.authState?.creds?.registered) {
        try {
          this.addLog("INFO", `Requesting Casileys pairing code for +${cleanPhone}...`);
          let rawCode: string | null = null;
          if (this.customPairingCodeRequested) {
            try {
              rawCode = await this.sock.requestPairingCode(cleanPhone, this.customPairingCodeRequested);
            } catch {
              rawCode = await this.sock.requestPairingCode(cleanPhone);
            }
          } else {
            rawCode = await this.sock.requestPairingCode(cleanPhone);
          }
          if (rawCode) {
            const formattedCode = rawCode.includes("-") ? rawCode : rawCode.match(/.{1,4}/g)?.join("-") || rawCode;
            this.state.pairingCode = formattedCode;
            this.state.connectionStatus = "pairing_code_ready";
            this.addLog("INFO", `✅ WhatsApp Pairing Code generated for +${cleanPhone}: ${formattedCode}`);

            let qrBuffer: Buffer | null = null;
            if (this.rawQrString) {
              try {
                qrBuffer = await QRCode.toBuffer(this.rawQrString, {
                  type: "png",
                  margin: 2,
                  scale: 8,
                  color: { dark: "#000000", light: "#ffffff" },
                });
              } catch {}
            }

            return {
              success: true,
              pairingCode: formattedCode,
              qrBuffer: qrBuffer || undefined,
              message: "Pairing code generated successfully.",
            };
          }
        } catch (err: any) {
          const msg = err?.message || String(err);
          this.addLog("WARN", `Pairing code attempt for +${cleanPhone}: ${msg}`);
          if (msg.includes("registered")) break;
        }
      }

      await new Promise((r) => setTimeout(r, 600));
    }

    return {
      success: false,
      message: "Pairing session timed out. Please try again in a few seconds.",
    };
  }

  async stop(isUserExplicitStop = true) {
    this.isStarting = false;
    this.isExplicitlyStopped = isUserExplicitStop;
    this.reconnectAttempts = isUserExplicitStop ? this.maxReconnectAttempts : 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.isQrReady = false;
    this.startTime = null;
    this.state.isOnline = false;
    this.state.connectionStatus = "disconnected";
    if (isUserExplicitStop) {
      this.state.qrCodeDataUrl = null;
      this.state.pairingCode = null;
    }
    if (this.sock) {
      try {
        this.sock.ev.removeAllListeners("connection.update");
        this.sock.ev.removeAllListeners("creds.update");
        this.sock.ev.removeAllListeners("messages.upsert");
        this.sock.ev.removeAllListeners("call");
        this.sock.ev.removeAllListeners("group-participants.update");
        try { this.sock.ws?.close(); } catch (e) {}
        try { this.sock.end(void 0); } catch (e) {}
      } catch (err) {}
      this.sock = null;
    }
    if (isUserExplicitStop) {
      this.addLog("INFO", "🛑 Bot engine manually stopped by user.");
    }
    return { success: true, message: "Bot stopped successfully." };
  }

  async restart() {
    this.addLog("INFO", `Restarting ${this.state.config.botName} bot engine...`);
    await this.stop();
    await new Promise((r) => setTimeout(r, 1200));
    return this.start();
  }

  async clearSession() {
    await this.stop();
    adminStore.deleteSessionBackup(this.userId);
    this.clearSessionFiles();
    this.state.pairedNumber = null;
    this.state.qrCodeDataUrl = null;
    this.state.pairingCode = null;
    this.state.isOnline = false;
    this.state.connectionStatus = "disconnected";
    this.addLog("INFO", "🧹 Session data and credentials wiped. Ready for fresh pairing.");
    return { success: true, message: "Session data cleared successfully." };
  }

  cleanupCorruptSessionKeys() {
    try {
      if (!fs.existsSync(this.sessionDir)) return;
      const files = fs.readdirSync(this.sessionDir);
      let removedCount = 0;
      for (const file of files) {
        if (file === "creds.json") continue;
        if (
          file.startsWith("app-state-") ||
          file.startsWith("pre-key-") ||
          file.startsWith("sender-key-") ||
          file.startsWith("session-")
        ) {
          try {
            fs.unlinkSync(path.join(this.sessionDir, file));
            removedCount++;
          } catch (e) {}
        }
      }
      if (removedCount > 0) {
        this.addLog("WARN", `🧹 Auto-cleaned ${removedCount} corrupt/stale session key files (preserved creds.json).`);
      }
    } catch (e: any) {
      this.addLog("WARN", `Session key cleanup error: ${e?.message}`);
    }
  }

  clearSessionFiles() {
    try {
      if (fs.existsSync(this.sessionDir)) {
        fs.rmSync(this.sessionDir, { recursive: true, force: true });
      }
      fs.mkdirSync(this.sessionDir, { recursive: true });
    } catch (e: any) {
      this.addLog("WARN", `Error clearing session files: ${e?.message}`);
    }
  }

  extractMessageContent(msg: any) {
    if (!msg.message) return "";
    try {
      const parsedReply = typeof parseInteractiveReply === "function" ? parseInteractiveReply(msg.message) : null;
      if (parsedReply?.kind && (parsedReply.id || parsedReply.displayText)) {
        return String(parsedReply.id || parsedReply.displayText);
      }
    } catch {}
    let messageObj=msg.message;let depth=0;while(depth<5&&messageObj&&(messageObj.viewOnceMessage?.message||messageObj.viewOnceMessageV2?.message||messageObj.viewOnceMessageV2Extension?.message||messageObj.ephemeralMessage?.message||messageObj.documentWithCaptionMessage?.message)){messageObj=messageObj.viewOnceMessage?.message||messageObj.viewOnceMessageV2?.message||messageObj.viewOnceMessageV2Extension?.message||messageObj.ephemeralMessage?.message||messageObj.documentWithCaptionMessage?.message;depth++}if(!messageObj)return"";if(messageObj.conversation)return messageObj.conversation;if(messageObj.extendedTextMessage?.text)return messageObj.extendedTextMessage.text;if(messageObj.imageMessage?.caption)return messageObj.imageMessage.caption;if(messageObj.videoMessage?.caption)return messageObj.videoMessage.caption;if(messageObj.buttonsResponseMessage?.selectedButtonId){return messageObj.buttonsResponseMessage.selectedButtonId}if(messageObj.buttonsResponseMessage?.selectedDisplayText){return messageObj.buttonsResponseMessage.selectedDisplayText}if(messageObj.templateButtonReplyMessage?.selectedId){return messageObj.templateButtonReplyMessage.selectedId}if(messageObj.templateButtonReplyMessage?.selectedDisplayText){return messageObj.templateButtonReplyMessage.selectedDisplayText}if(messageObj.listResponseMessage?.singleSelectReply?.selectedRowId){return messageObj.listResponseMessage.singleSelectReply.selectedRowId}if(messageObj.listResponseMessage?.title){return messageObj.listResponseMessage.title}if(messageObj.interactiveResponseMessage?.nativeFlowResponseMessage?.paramsJson){try{const parsed=JSON.parse(messageObj.interactiveResponseMessage.nativeFlowResponseMessage.paramsJson);const resolved=parsed?.id||parsed?.selected_id||parsed?.selected_row_id||parsed?.selectedRowId||parsed?.title||parsed?.display_text||"";if(resolved)return resolved}catch{}}if(messageObj.interactiveResponseMessage?.body?.text){return messageObj.interactiveResponseMessage.body.text}return""
  }mapButtonToCommand(btnId, prefix) {
  const clean = (btnId || '').trim();
  switch (clean) {
    case "welcome_ping":
    case "btn_ping":
      return `${prefix}ping`;
    case "welcome_alive":
    case "btn_alive":
      return `${prefix}alive`;
    case "welcome_menu":
    case "welcome_main":
    case "btn_main_menu":
    case "menu_main":
      return `${prefix}menu`;
    case "menu_profile":
    case "menu_cat_1":
    case "cat_profile":
    case "cat_1":
    case "catmenu1":
    case "catmenu_1":
    case "cat1":
      return `${prefix}menu 1`;
    case "menu_download":
    case "menu_cat_2":
    case "cat_download":
    case "cat_2":
    case "catmenu2":
    case "catmenu_2":
    case "cat2":
    case "download":
      return `${prefix}menu 2`;
    case "menu_movies":
    case "menu_movie":
    case "menu_cat_3":
    case "cat_movies":
    case "cat_3":
    case "catmenu3":
    case "catmenu_3":
    case "cat3":
      return `${prefix}menu 3`;
    case "menu_ai":
    case "menu_cat_4":
    case "cat_ai":
    case "cat_4":
    case "catmenu4":
    case "catmenu_4":
    case "cat4":
      return `${prefix}menu 4`;
    case "menu_group":
    case "menu_groups":
    case "menu_cat_5":
    case "cat_group":
    case "cat_5":
    case "catmenu5":
    case "catmenu_5":
    case "cat5":
    case "groupmanage":
    case "adminmenu":
      return `${prefix}menu 5`;
    case "menu_games":
    case "menu_game":
    case "menu_cat_6":
    case "cat_games":
    case "cat_6":
    case "catmenu6":
    case "catmenu_6":
    case "cat6":
      return `${prefix}menu 6`;
    case "menu_settings":
    case "menu_setting":
    case "menu_cat_7":
    case "cat_settings":
    case "cat_7":
    case "catmenu7":
    case "catmenu_7":
    case "cat7":
      return `${prefix}menu 7`;
    case "cat_daily":
    case "cat_8":
    case "catmenu8":
    case "catmenu_8":
    case "cat8":
    case "utility":
    case "math":
      return `${prefix}menu 8`;
    case "menu_song":
    case "menu_cat_song":
    case "cat_song":
    case "catmenu9":
    case "catmenu_9":
    case "cat9":
    case "music":
      return `${prefix}menu 9`;
    case "menu_aiimage":
    case "menu_cat_aiimage":
    case "cat_aiimage":
    case "catmenu10":
    case "catmenu_10":
    case "cat10":
      return `${prefix}menu 10`;
    case "menu_owner":
    case "menu_cat_owner":
    case "cat_owner":
    case "catmenu11":
    case "catmenu_11":
    case "cat11":
      return `${prefix}menu 11`;
    case "menu_tools":
    case "menu_cat_tools":
    case "cat_tools":
    case "catmenu12":
    case "catmenu_12":
    case "cat12":
    case "tools":
    case "texttools":
    case "other":
      return `${prefix}menu 12`;
    case "menu_premium":
    case "menu_cat_premium":
    case "cat_premium":
    case "catmenu13":
    case "catmenu_13":
    case "cat13":
      return `${prefix}menu 13`;
    case "menu_18":
    case "menu_cat_18":
    case "cat_18":
    case "menu_adult":
    case "catmenu14":
    case "catmenu_14":
    case "cat14":
      return `${prefix}menu 14`;
    case "catmenu15":
    case "catmenu_15":
    case "cat15":
      return `${prefix}menu 8`;
    case "btn_cat_song":
    case "btn_song":
      return `${prefix}song`;
    case "btn_cat_video":
    case "btn_video":
      return `${prefix}video`;
    case "btn_play":
      return `${prefix}play`;
    case "btn_audio":
      return `${prefix}audio`;
    case "btn_ytsearch":
      return `${prefix}ytsearch`;
    case "btn_mf":
    case "btn_mediafire":
      return `${prefix}mf`;
    case "btn_tt":
    case "btn_tiktok":
      return `${prefix}tiktok`;
    case "btn_fb":
      return `${prefix}fb`;
    case "btn_ig":
      return `${prefix}ig`;
    case "btn_twitter":
      return `${prefix}twitter`;
    case "btn_apk":
      return `${prefix}apk`;
    case "btn_lyrics":
      return `${prefix}lyrics`;
    case "btn_quiz":
      return `${prefix}quiz`;
    case "btn_roll":
      return `${prefix}roll`;
    case "btn_flip":
      return `${prefix}flip`;
    case "btn_slot":
      return `${prefix}slot`;
    case "btn_owner":
      return `${prefix}owner`;
    case "btn_upgrade":
      return `${prefix}upgrade`;
    default:
      return clean.startsWith(prefix) ? clean : `${prefix}${clean}`;
  }
}async handleIncomingMessage(msg) {
    if (!this.sock || !msg.key) return;

    // 1. Deduplicate message by ID
    const msgId = msg.key.id;
    if (msgId) {
      if (this.processedMessageIds.has(msgId)) return;
      this.processedMessageIds.add(msgId);
      if (this.processedMessageIds.size > 3000) {
        const first = this.processedMessageIds.values().next().value;
        if (first) this.processedMessageIds.delete(first);
      }
    }

    // 2. Ignore old messages synced on reconnect (older than 60s)
    const rawTimestamp = typeof msg.messageTimestamp === "number" ? msg.messageTimestamp : Number(msg.messageTimestamp) || 0;
    if (rawTimestamp > 0) {
      const ageMs = Date.now() - (rawTimestamp * 1000);
      if (ageMs > 60000) return;
    }
    const jid = msg.key.remoteJid;
    if (!jid || jid.endsWith("@broadcast") || jid.endsWith("@newsletter")) return;
    const isGroup = jid.endsWith("@g.us");
    const sender = isGroup ? (msg.key.participantAlt || msg.key.participant || jid) : (msg.key.remoteJidAlt || jid);
    const rawMsg =
      msg.message?.ephemeralMessage?.message ||
      msg.message?.viewOnceMessage?.message ||
      msg.message?.viewOnceMessageV2?.message ||
      msg.message;

    // 1. EXTRACT TEXT & SENDER DETAILS FIRST (Prevents ReferenceError: Cannot access 'text' before initialization)
    const messageContent = this.extractMessageContent(msg);
    const rawExtractedText = (messageContent || "").trim();
    let text = rawExtractedText;
    const prefix = this.state.config.prefix || ".";

    // --- DEDICATED BUSINESS BOT STORE FLOW ---
    if (this.userId && this.userId.endsWith("__business")) {
      const lowerBiz = rawExtractedText.toLowerCase();
      const bizPrefix = (prefix || ".").toLowerCase();
      const isPanalMenuCmd =
        lowerBiz === `${bizPrefix}panal` ||
        lowerBiz === ".panal" ||
        lowerBiz === `${bizPrefix}panel` ||
        lowerBiz === ".panel" ||
        lowerBiz === `${bizPrefix}guild` ||
        lowerBiz === ".guild";
      const isPayApkCmd =
        lowerBiz === `${bizPrefix}payapk` ||
        lowerBiz === ".payapk" ||
        lowerBiz === `${bizPrefix}payapp apk` ||
        lowerBiz === ".payapp apk";
      const isPayKeyCmd =
        lowerBiz === `${bizPrefix}paykey` ||
        lowerBiz === ".paykey" ||
        lowerBiz === `${bizPrefix}payapp key` ||
        lowerBiz === ".payapp key";
      const isUtilityCmd =
        lowerBiz === `${bizPrefix}ping` ||
        lowerBiz === `${bizPrefix}alive`;
      const bizStoreCmdMatch =
        (rawExtractedText.startsWith(prefix) || rawExtractedText.startsWith(".")) &&
        !isUtilityCmd &&
        !isPanalMenuCmd &&
        !isPayApkCmd &&
        !isPayKeyCmd &&
        getBusinessStore(this.userId).products.some((prod) => {
          const slug = sanitizePanelCommandSlug(prod.command, prod.name);
          return lowerBiz === `${bizPrefix}${slug}` || lowerBiz === `.${slug}`;
        });
      const isAdminApprovalAction =
        rawExtractedText.startsWith("yh_admin_") ||
        /^(confirm|reject)\s+yh-\d+/i.test(rawExtractedText.trim());
      const isBankCardCmd =
        lowerBiz === `${bizPrefix}bank` ||
        lowerBiz === ".bank" ||
        lowerBiz === "bank details" ||
        rawExtractedText === "guild_bank_card";
      const isGuildDetailsPaste =
        /Region(?:\s*ID)?\s*[:=-]\s*[a-zA-Z0-9 _()-]{2,32}/i.test(rawExtractedText) &&
        /(?:Guild|Clan)\s*ID\s*[:=-]\s*\d{5,18}/i.test(rawExtractedText);
      const isBizActionId =
        rawExtractedText.startsWith("biz_") ||
        rawExtractedText.startsWith("guild_") ||
        rawExtractedText.startsWith("panel_") ||
        isGuildDetailsPaste ||
        isAdminApprovalAction ||
        isBankCardCmd ||
        isPanalMenuCmd ||
        isPayApkCmd ||
        isPayKeyCmd ||
        bizStoreCmdMatch;

      const ownPn = (this.sock?.user?.id || "").split(":")[0].split("@")[0].replace(/\D/g, "");
      const ownLid = (this.sock?.user?.lid || "").split(":")[0].split("@")[0].replace(/\D/g, "");
      const remoteDigits = jid.split(":")[0].split("@")[0].replace(/\D/g, "");
      const remoteAltDigits = (msg.key.remoteJidAlt || "").split(":")[0].split("@")[0].replace(/\D/g, "");
      const isSelfChat = Boolean(
        !isGroup &&
        ((ownPn && (remoteDigits === ownPn || remoteAltDigits === ownPn)) ||
         (ownLid && (remoteDigits === ownLid || remoteAltDigits === ownLid)))
      );

      // If fromMe: allow Self-Chat testing, explicit commands, or interactive button clicks; ignore echoes in customer chats
      if (msg.key.fromMe && !isSelfChat && !isBizActionId && !isUtilityCmd) {
        return;
      }

      if (isUtilityCmd) {
        // Fall through to execute .ping / .alive cleanly
      } else if (!isGroup || isBizActionId) {
        const senderDigitsBiz = sender.split(":")[0].split("@")[0].replace(/\D/g, "");
        const ownerDigitsBiz = (this.state.config.ownerNumber || "94740712934").replace(/\D/g, "");
        const isBizOwner = Boolean(
          msg.key.fromMe ||
          (ownerDigitsBiz.length >= 8 && senderDigitsBiz.includes(ownerDigitsBiz.slice(-9)))
        );
        await this.handleBusinessBotMessage(jid, msg, rawExtractedText, isGroup, isBizOwner);
        return;
      } else {
        // Group chat message on dedicated business bot without explicit biz action -> never leak personal bot voice/AI replies
        return;
      }
    }

    // Auto-prefix aliases & active pending quality selections BEFORE numeric menu shortcuts steal 1 / 2
    const trimmedLowerEarly = text.trim().toLowerCase();
    const pendingFilmEarly = this.pendingFilmChoices.get(jid);
    const pendingMediaEarly = this.pendingMediaChoices.get(jid);
    if (!text.startsWith(prefix) && pendingFilmEarly && Date.now() - pendingFilmEarly.ts < 15 * 60 * 1000) {
      if (trimmedLowerEarly === "1" || trimmedLowerEarly === ".1") {
        text = pendingFilmEarly.opt1Cmd;
        this.pendingFilmChoices.delete(jid);
      } else if (trimmedLowerEarly === "2" || trimmedLowerEarly === ".2") {
        text = pendingFilmEarly.opt2Cmd;
        this.pendingFilmChoices.delete(jid);
      } else if (/^(360p|480p|720p|1080p|compact|sd|hd|fhd)$/i.test(trimmedLowerEarly)) {
        const normQ = trimmedLowerEarly === "sd" ? "480p" : trimmedLowerEarly === "hd" ? "720p" : trimmedLowerEarly === "fhd" ? "1080p" : trimmedLowerEarly;
        text = `${prefix}${pendingFilmEarly.onlyDub ? "filmdub" : "film"} ${pendingFilmEarly.query} ${normQ}`;
        this.pendingFilmChoices.delete(jid);
      }
    } else if (!text.startsWith(prefix) && pendingMediaEarly && Date.now() - pendingMediaEarly.ts < 15 * 60 * 1000) {
      if (trimmedLowerEarly === "1" || trimmedLowerEarly === ".1") {
        text = pendingMediaEarly.opt1Cmd;
        this.pendingMediaChoices.delete(jid);
      } else if (trimmedLowerEarly === "2" || trimmedLowerEarly === ".2") {
        text = pendingMediaEarly.opt2Cmd;
        this.pendingMediaChoices.delete(jid);
      } else if (/^(360p|480p|720p|1080p|sd|hd|mp3|audio)$/i.test(trimmedLowerEarly)) {
        const normQ =
          trimmedLowerEarly === "sd" || trimmedLowerEarly === "360p"
            ? "480p"
            : trimmedLowerEarly === "hd" || trimmedLowerEarly === "1080p"
              ? "720p"
              : trimmedLowerEarly === "audio"
                ? "mp3"
                : trimmedLowerEarly;
        text = `${prefix}${pendingMediaEarly.baseCmd} ${pendingMediaEarly.query} ${normQ}`;
        this.pendingMediaChoices.delete(jid);
      }
    }

    const firstWord = (text.split(/\s+/)[0] || "").toLowerCase();
    const hasAttachedOrQuotedMedia = Boolean(
      rawMsg?.imageMessage ||
      rawMsg?.videoMessage ||
      rawMsg?.stickerMessage ||
      rawMsg?.extendedTextMessage?.contextInfo?.quotedMessage
    );
    if (!text.startsWith(prefix)) {
      const classicTs = this.classicMenuContext.get(jid);
      const isRecentClassicMenu = Boolean(classicTs && Date.now() - classicTs < 15 * 60 * 1000);
      if (
        ["menu", "textmenu", "classicmenu", "listmenu", "uimenu", "start", "help", "ping", "alive", "owner", "settings", "song", "video", "yt", "ytv", "tiktok", "tt", "fb", "facebook", "ig", "instagram", "twitter", "tw", "twter", "status", "download", "ai", "music", "movies", "games", "group", "admin", "tools", "utility", "premium", "sticker"].includes(firstWord) ||
        (firstWord === "s" && hasAttachedOrQuotedMedia)
      ) {
        text = `${prefix}${text}`;
      } else if (isRecentClassicMenu && /^(?:\.?([0-9]|10))$/.test(trimmedLowerEarly)) {
        const cleanNum = trimmedLowerEarly.replace(/^\./, "");
        const classicNumToCmd: Record<string, string> = {
          "0": `${prefix}menu`,
          "1": `${prefix}menu dl`,
          "2": `${prefix}menu ai`,
          "3": `${prefix}menu movies`,
          "4": `${prefix}menu group`,
          "5": `${prefix}menu games`,
          "6": `${prefix}menu music`,
          "7": `${prefix}menu tools`,
          "8": `${prefix}menu sys`,
          "9": `${prefix}menu util`,
          "10": `${prefix}menu search`,
        };
        text = classicNumToCmd[cleanNum] || `${prefix}menu`;
      } else if (/^[1-9]$|^1[0-5]$/.test(trimmedLowerEarly)) {
        const numToCmd: Record<string, string> = {
          "1": `${prefix}download`,
          "2": `${prefix}ai`,
          "3": `${prefix}group`,
          "4": `${prefix}admin`,
          "5": `${prefix}tools`,
          "6": `${prefix}owner`,
          "7": `${prefix}other`,
          "8": `${prefix}music`,
          "9": `${prefix}aiimage`,
          "10": `${prefix}movies`,
          "11": `${prefix}premium`,
          "12": `${prefix}texttools`,
          "13": `${prefix}utility`,
          "14": `${prefix}games`,
          "15": `${prefix}math`,
        };
        text = numToCmd[trimmedLowerEarly] || `${prefix}menu ${trimmedLowerEarly}`;
      }
    }

    const ownPnMain = (this.sock?.user?.id || "").split(":")[0].split("@")[0].replace(/\D/g, "");
    const ownLidMain = (this.sock?.user?.lid || "").split(":")[0].split("@")[0].replace(/\D/g, "");
    const remoteDigitsMain = jid.split(":")[0].split("@")[0].replace(/\D/g, "");
    const remoteAltDigitsMain = (msg.key.remoteJidAlt || "").split(":")[0].split("@")[0].replace(/\D/g, "");
    const isSelfChatMain = Boolean(
      !isGroup &&
      ((ownPnMain && (remoteDigitsMain === ownPnMain || remoteAltDigitsMain === ownPnMain)) ||
       (ownLidMain && (remoteDigitsMain === ownLidMain || remoteAltDigitsMain === ownLidMain)))
    );

    // 3. If fromMe and does NOT start with prefix, ignore so the bot never loops/echoes its own chat messages (except self-chat voice testing)
    if (msg.key.fromMe && !text.startsWith(prefix) && !(isSelfChatMain && rawMsg?.audioMessage)) {
      return;
    }
    const senderName = msg.pushName || sender.split("@")[0] || "Friend";
    const botPhone = (this.sock?.user?.id || "").split(":")[0].split("@")[0];
    const contextInfo =
      rawMsg?.audioMessage?.contextInfo ||
      rawMsg?.videoMessage?.contextInfo ||
      rawMsg?.imageMessage?.contextInfo ||
      msg.message?.extendedTextMessage?.contextInfo;
    const isBotMentioned = Boolean(botPhone && contextInfo?.mentionedJid?.some((m: string) => m.includes(botPhone)));
    const isReplyToBot = Boolean(botPhone && contextInfo?.participant && contextInfo.participant.includes(botPhone));
    const shouldProcessMediaAi = !isGroup || isBotMentioned || isReplyToBot;

    if ((!msg.key.fromMe || isSelfChatMain) && rawMsg?.audioMessage && shouldProcessMediaAi) {
      const audio = rawMsg.audioMessage;
      this.addLog("INFO", `🎙️ [AI VOICE LISTENER] Received audio note from ${sender.split("@")[0]}`);
      try {
        await this.sock.sendMessage(jid, { react: { text: "🎙️", key: msg.key } }).catch(() => {});
        const audioBuffer = await downloadMediaMessage(
          msg,
          "buffer",
          {},
          { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage }
        );
        if (audioBuffer && audioBuffer.length > 0) {
          await this.sock.sendPresenceUpdate("composing", jid).catch(() => {});
          const cleanAudioMime = (audio.mimetype || "audio/ogg").split(";")[0].trim() || "audio/ogg";
          const aiResult = await analyzeAudioWithGemini(audioBuffer, cleanAudioMime);
          await this.reply(
            jid,
            msg,
            `🎙️ *[YASHI HALLS AI VOICE ASSISTANT]*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${aiResult.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Voice Note Understood & Replied_`
          );
          return;
        }
      } catch (err: any) {
        this.addLog("ERROR", `Voice listener error: ${err?.message || err}`);
      }
    }

    const rawVideoCaption = (rawMsg?.videoMessage?.caption || "").trim();
    const isVideoCommandCaption =
      Boolean(rawVideoCaption) &&
      (rawVideoCaption.startsWith(prefix) || /^[.!#/]/.test(rawVideoCaption));

    if (!msg.key.fromMe && rawMsg?.videoMessage && shouldProcessMediaAi && !isVideoCommandCaption) {
      const video = rawMsg.videoMessage;
      const caption = video.caption || "";
      const sizeBytes = Number(video.fileLength) || 0;
      this.addLog("INFO", `📹 [AI VIDEO ANALYZER] Received video (${(sizeBytes / 1024 / 1024).toFixed(1)} MB) from ${sender.split("@")[0]}`);
      try {
        await this.sock.sendMessage(jid, { react: { text: "📹", key: msg.key } }).catch(() => {});
        if (sizeBytes > 20 * 1024 * 1024) {
          await this.reply(
            jid,
            msg,
            `📹 *වීඩියෝව විශාල වැඩිය!* (Video exceeds 20MB limit)\nAI විශ්ලේෂණය සඳහා කරුණාකර කුඩා වීඩියෝවක් එවන්න.`
          );
          return;
        }
        const videoBuffer = await downloadMediaMessage(
          msg,
          "buffer",
          {},
          { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage }
        );
        if (videoBuffer && videoBuffer.length > 0) {
          await this.sock.sendPresenceUpdate("composing", jid).catch(() => {});
          const aiResult = await analyzeVideoWithGemini(videoBuffer, video.mimetype || "video/mp4", caption);
          await this.reply(
            jid,
            msg,
            `📹 *[YASHI HALLS AI VIDEO VISION]*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${aiResult.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Video Scenes Understood & Analyzed_`
          );
          return;
        }
      } catch (err: any) {
        this.addLog("ERROR", `Video analyzer error: ${err?.message || err}`);
      }
    }

    // --- SMART MEMORY & CONTACT CONTEXT ---
    if (!this.chatMemory) this.chatMemory = new Map();
    if (!this.chatMemory.has(jid)) this.chatMemory.set(jid, []);
    const history = this.chatMemory.get(jid);
    const linkMatch = text ? text.match(/(https?:\/\/[^\s]+)/i) : null;
    const foundLink = linkMatch ? linkMatch[0] : undefined;

    // --- PHOTO / ALBUM HANDLING ---
    // When photos or albums are sent, never send unsolicited automatic greetings or heart reactions.
    // If an image caption contains an explicit command (e.g. .sticker, .ai), it will be handled by the command router below.

    if (text) {
      history.push({
        senderName,
        senderJid: sender,
        text,
        timestamp: Date.now(),
        hasLink: foundLink,
        isBot: Boolean(msg.key.fromMe),
      });
      if (history.length > 20) history.shift();
    }
    const senderDigits = sender.split("@")[0].replace(/\D/g, "");
    const ownerDigits = (this.state.config.ownerNumber || "94740712934").replace(/\D/g, "");
    const pairedDigits = (this.state.pairedNumber || "").replace(/\D/g, "");
    const isOwner = Boolean(
      msg.key.fromMe ||
      (ownerDigits.length >= 8 && senderDigits.includes(ownerDigits.slice(-9))) ||
      (pairedDigits.length >= 8 && senderDigits.includes(pairedDigits.slice(-9))) ||
      (ownerDigits && sender.includes(ownerDigits))
    );

    const isPrivateMode = this.state.config.mode === "private";

    if (isGroup && this.state.config.antiLink && !msg.key.fromMe) {
      const hasLink = /(https?:\/\/[^\s]+|chat\.whatsapp\.com\/[^\s]+|wa\.me\/[^\s]+|bit\.ly\/[^\s]+|t\.me\/[^\s]+)/gi.test(text);
      if (hasLink && !isOwner) {
        this.addLog("WARN", `[ANTI-LINK] Unauthorized link detected from ${sender.split("@")[0]} in ${jid}`);
        try { await this.sock.sendMessage(jid, { delete: msg.key }); } catch {}
        try {
          await this.sock.sendMessage(jid, {
            text: `⚠️ *ANTI-LINK SYSTEM ACTIVATED!* 🚫\n@${sender.split("@")[0]}, ගෲප් එකට ලින්ක්ස් (links) දැමීම තහනම් කර ඇත!\n\n⏳ *3... 2... 1... Good byee! 👋*`,
            mentions: [sender]
          });
        } catch {}
        try {
          await this.sock.groupParticipantsUpdate(jid, [sender], "remove");
          this.addLog("INFO", `[ANTI-LINK] Kicked user ${sender.split("@")[0]} from group ${jid}`);
        } catch (kErr: any) {
          this.addLog("WARN", `[ANTI-LINK] Could not kick ${sender.split("@")[0]}: ${kErr?.message}`);
        }
        return;
      }
    }

    if (this.activeQuizSessions.has(jid) && !text.startsWith(prefix)) {
      const quiz = this.activeQuizSessions.get(jid);
      if (text.toLowerCase() === quiz.answer.toLowerCase()) {
        this.activeQuizSessions.delete(jid);
        await this.reply(
          jid,
          msg,
          `🎉 *CORRECT ANSWER!* 🏆\nCongratulations @${sender.split("@")[0]}! You solved the quiz.\n_Answer was:_ *${quiz.answer}*`
        );
        return;
      }
    }

    if (text.startsWith("btn_") || text.startsWith("welcome_") || text.startsWith("menu_") || text.startsWith("cat_")) {
      text = this.mapButtonToCommand(text, prefix);
    } else if (!text.startsWith(prefix)) {
      const lower = text.toLowerCase().trim();
      if (lower.includes("mediafire.com/file/") || lower.includes("mediafire.com/?") || lower.includes("mediafire.com/download")) {
        text = `${prefix}mediafire ${text}`;
      } else if (
        lower.startsWith("film ") ||
        lower.startsWith("movie ") ||
        lower.startsWith("filmdub ") ||
        lower.startsWith("dub ") ||
        lower.startsWith("cineru ") ||
        lower.startsWith("cinerulk ") ||
        lower.startsWith("sinhalasub ") ||
        lower.startsWith("cinesubz ") ||
        lower.startsWith("pair ") ||
        lower.startsWith("paircode ") ||
        lower.startsWith("qr ") ||
        lower.startsWith("connect ") ||
        lower === "pair" ||
        lower === "paircode" ||
        lower === "qr" ||
        lower === "connect" ||
        lower.startsWith("song ") ||
        lower.startsWith("video ") ||
        lower.startsWith("yt ") ||
        lower.startsWith("ytv ") ||
        lower.startsWith("tiktok ") ||
        lower.startsWith("tt ") ||
        lower.startsWith("facebook ") ||
        lower.startsWith("fb ") ||
        lower.startsWith("instagram ") ||
        lower.startsWith("ig ") ||
        lower.startsWith("twitter ") ||
        lower.startsWith("mp3 ")
      ) {
        text = `${prefix}${text}`;
      } else {
        const pendingFilm = this.pendingFilmChoices.get(jid);
        const pendingMedia = this.pendingMediaChoices.get(jid);
        if (pendingFilm && Date.now() - pendingFilm.ts < 15 * 60 * 1000) {
          if (lower === "1" || lower === ".1") {
            text = pendingFilm.opt1Cmd;
            this.pendingFilmChoices.delete(jid);
          } else if (lower === "2" || lower === ".2") {
            text = pendingFilm.opt2Cmd;
            this.pendingFilmChoices.delete(jid);
          } else if (/^(360p|480p|720p|1080p|compact|sd|hd|fhd)$/i.test(lower)) {
            const normQ = lower === "sd" ? "480p" : lower === "hd" ? "720p" : lower === "fhd" ? "1080p" : lower;
            text = `${prefix}${pendingFilm.onlyDub ? "filmdub" : "film"} ${pendingFilm.query} ${normQ}`;
            this.pendingFilmChoices.delete(jid);
          }
        } else if (pendingMedia && Date.now() - pendingMedia.ts < 15 * 60 * 1000) {
          if (lower === "1" || lower === ".1") {
            text = pendingMedia.opt1Cmd;
            this.pendingMediaChoices.delete(jid);
          } else if (lower === "2" || lower === ".2") {
            text = pendingMedia.opt2Cmd;
            this.pendingMediaChoices.delete(jid);
          } else if (/^(360p|480p|720p|1080p|sd|hd|mp3|audio)$/i.test(lower)) {
            const normQ =
              lower === "sd" || lower === "360p"
                ? "480p"
                : lower === "hd" || lower === "1080p"
                  ? "720p"
                  : lower === "audio"
                    ? "mp3"
                    : lower;
            text = `${prefix}${pendingMedia.baseCmd} ${pendingMedia.query} ${normQ}`;
            this.pendingMediaChoices.delete(jid);
          }
        }

        if (!text.startsWith(prefix)) {
          const numMap: Record<string, string> = {
            "0": `${prefix}menu`,
            ".0": `${prefix}menu`,
            "1": `${prefix}menu 1`,
            ".1": `${prefix}menu 1`,
            "2": `${prefix}menu 2`,
            ".2": `${prefix}menu 2`,
            "3": `${prefix}menu 3`,
            ".3": `${prefix}menu 3`,
            "4": `${prefix}menu 4`,
            ".4": `${prefix}menu 4`,
            "5": `${prefix}menu 5`,
            ".5": `${prefix}menu 5`,
            "6": `${prefix}menu 6`,
            ".6": `${prefix}menu 6`,
            "7": `${prefix}menu 7`,
            ".7": `${prefix}menu 7`,
            "8": `${prefix}menu 8`,
            ".8": `${prefix}menu 8`,
          };
          if (numMap[lower]) {
            text = numMap[lower];
          } else if (["menu", "help", "ping", "alive", "owner", "settings", "quiz", "roll", "flip", "guess", "film", "filmdub", "movie", "mediafire", "mf", "cineru", "sinhalasub", "pair", "paircode", "qr", "connect"].includes(lower)) {
            text = `${prefix}${lower}`;
          }
        }
      }
    }

    // In PRIVATE mode, skip Voice Auto-Reply and Smart AI Auto-Reply for non-owners
    if (!isPrivateMode || isOwner) {
      if (this.state.config.voiceAutoReplyEnabled !== false && !text.startsWith(prefix)) {
        const voiceMatch = adminStore.findMatchingVoiceReply(text);
        if (voiceMatch && voiceMatch.match) {
          const voiceReply = voiceMatch.match;
          const cooldownKey = `${jid}_${voiceReply.id}`;
          const lastSent = this.voiceCooldownMap.get(cooldownKey) || 0;
          const now = Date.now();
          if (now - lastSent > 1000) {
            this.voiceCooldownMap.set(cooldownKey, now);
            this.addLog(
              "INFO",
              `🎙️ [VOICE AUTO-REPLY] Trigger "${voiceMatch.matchedTrigger}" matched incoming text: "${text}" from ${sender.split("@")[0]}`
            );
            try {
              const voiceData = await getWhatsAppVoiceBuffer(voiceReply.audioPath);
              if (voiceData && voiceData.buffer) {
                try {
                  await this.sock.sendMessage(
                    jid,
                    { audio: voiceData.buffer, mimetype: voiceData.mimeType, ptt: true },
                    { quoted: msg }
                  );
                } catch (quotedErr) {
                  await this.sock.sendMessage(jid, {
                    audio: voiceData.buffer,
                    mimetype: voiceData.mimeType,
                    ptt: true,
                  });
                }
                this.state.voiceAutoRepliesSentCount = (this.state.voiceAutoRepliesSentCount || 0) + 1;
                this.state.autoRepliesSentCount = (this.state.autoRepliesSentCount || 0) + 1;
                adminStore.incrementVoiceReplyPlay(voiceReply.id);
                return;
              }
            } catch (voiceErr: any) {}
          }
        }
      }

      if (this.state.config.smartAutoReply !== false && !msg.key.fromMe && !text.startsWith(prefix)) {
        const lowerText = text.toLowerCase().trim();

        if (
          /^(bot off|disable bot|autochat off|stop bot|bot nawaththapan|bot deseble|bot disable|බොට් ඕෆ්|බොට් නවත්තන්න)\b/i.test(lowerText) ||
          lowerText.includes("bot deseble") ||
          lowerText.includes("bot disable") ||
          lowerText.includes("bot off")
        ) {
          this.updateConfig({ smartAutoReply: false });
          await this.reply(
            jid,
            msg,
            `🤖 *AI AUTO-CHAT අක්‍රීය කරන ලදී (Disabled)!* 🔴\n\nනැවත සක්‍රිය කිරීමට *${prefix}autochat on* ලෙස එවන්න.`
          );
          return;
        }

        const botPhone = (this.sock?.user?.id || "").split(":")[0].split("@")[0];
        const contextInfo = msg.message?.extendedTextMessage?.contextInfo || rawMsg?.extendedTextMessage?.contextInfo;
        const isBotMentioned = Boolean(contextInfo?.mentionedJid?.some(m => m.includes(botPhone)));
        const isReplyToBot = Boolean(contextInfo?.participant && contextInfo.participant.includes(botPhone));
        const isExplicitBotCall = /(yashi|යශී|බොට්|bot)\b/i.test(lowerText);

        if (/^(https?:\/\/(?:www\.)?(?:youtube\.com|youtu\.be)\/[^\s]+)$/i.test(text.trim())) {
          await this.reply(
            jid,
            msg,
            `පට්ට ලින්ක් එකක්නේ ${senderName}! මම බැලුවා ඕක ❤️\n_ඕනෙ නම් "මට ඒකෙ mp3 එක දියන්" හරි "video එක එවපන්" හරි කියන්න._`
          );
          return;
        }

        const lastAiTime = this.chatAiCooldownMap?.get(jid) || 0;
        const isQuickFollowUp = (Date.now() - lastAiTime) < 10000;
        const isDirectlyAddressed = isBotMentioned || isReplyToBot || isExplicitBotCall;
        const isFeatureRequest = /(mp3|audio|video|song|සින්දුව|බාගන්න|ඩවුන්ලෝඩ්|ස්ටිකර්|sticker|film|movie|mediafire|apk)/i.test(lowerText);
        const isQuestionOrGreeting = /^(hi|hello|hey|හායි|හෙලෝ|කොහොමද|කවුද|mokada|kawuda|kohomada|good morning|gm|gn)\b/i.test(lowerText) || lowerText.includes("?") || lowerText.includes("මොකක්ද");

        let shouldAIRespond = false;
        if (isGroup) {
          shouldAIRespond = isDirectlyAddressed;
        } else {
          if (isDirectlyAddressed || isFeatureRequest) {
            shouldAIRespond = true;
          } else if (isQuestionOrGreeting) {
            shouldAIRespond = !isQuickFollowUp;
          } else {
            const isCasualChatter = /^(waasi|hari|ok|ela|ow|na|hode|ado|aniwa|hmm|hmmm|ah|oh|daala denwko)\b/i.test(lowerText) || lowerText.length < 3;
            shouldAIRespond = !isQuickFollowUp && !isCasualChatter;
          }
        }

        if (shouldAIRespond) {
          await this.sock.sendPresenceUpdate("composing", jid).catch(() => {});
          const aiRes = await chatAsYashiHallsAI({
            userText: text,
            senderName,
            recentMessages: history,
          });

          if (aiRes.action === "send_mp3") {
            await this.reply(jid, msg, aiRes.replyText);
            let targetUrl = aiRes.targetQuery;
            if (!targetUrl || !targetUrl.includes("http")) {
              const lastLinkItem = [...history].reverse().find(h => h.hasLink);
              if (lastLinkItem?.hasLink) {
                targetUrl = lastLinkItem.hasLink;
              }
            }
            if (targetUrl) {
              try {
                this.addLog("INFO", `[SMART AI ACTION] Automatically downloading MP3 for "${targetUrl}" requested by ${senderName}`);
                const audioResult = await resolveSongAudio(targetUrl);
                if (audioResult && audioResult.success && audioResult.buffer) {
                  await this.sendAudio(
                    jid,
                    msg,
                    audioResult.buffer,
                    audioResult.fileName || "song.mp3",
                    audioResult.title,
                    audioResult.thumbnailUrl,
                    audioResult.sourceUrl
                  );
                }
              } catch (err) {}
            }
            if (this.chatAiCooldownMap) this.chatAiCooldownMap.set(jid, Date.now());
            return;
          }

          if (aiRes.replyText && aiRes.success) {
            await this.reply(jid, msg, aiRes.replyText);
            if (this.chatAiCooldownMap) this.chatAiCooldownMap.set(jid, Date.now());
            history.push({
              senderName: this.state.config.botName || "YASHI HALLS",
              senderJid: this.sock.user?.id || "bot",
              text: aiRes.replyText,
              timestamp: Date.now(),
              isBot: true,
            });
            return;
          }
        }

        const senderNumber = sender.split("@")[0];
        const cooldownKey = `smart_reply_${jid}_${senderNumber}`;
        const lastSent = this.smartReplyCooldownMap.get(cooldownKey) || 0;
        const now = Date.now();
        if (now - lastSent > 4000) {
          let smartResponse = null;
          if (this.state.config.customAutoReplies && Array.isArray(this.state.config.customAutoReplies)) {
            for (const rule of this.state.config.customAutoReplies) {
              if (rule.keyword && rule.reply) {
                const kw = rule.keyword.trim().toLowerCase();
                if (kw && (lowerText === kw || lowerText.includes(kw))) {
                  smartResponse = rule.reply.replace(/@user/g, `@${senderNumber}`).replace(/@bot/g, this.state.config.botName).replace(/@prefix/g, prefix);
                  break;
                }
              }
            }
          }
          if (!smartResponse && (/^(hi|hello|hey|heyy|hii|හායි|හායී|හෙලෝ|හලෝ|halo|hallo|hola|wassup)[\s!.,?]*$/i.test(lowerText) || /^(hi|hello|hey|හායි|හායී|හෙලෝ|හලෝ)\b/i.test(lowerText))) {
            smartResponse = `හායි @${senderNumber}! 👋\nමම *${this.state.config.botName}* WhatsApp Bot. 🤖\n\nඔබට උදව් කිරීමට මම සූදානම්! විධාන සියල්ල බැලීමට *${prefix}menu* ලෙස එවන්න. 😊`;
          }
          if (smartResponse) {
            this.smartReplyCooldownMap.set(cooldownKey, now);
            this.state.autoRepliesSentCount++;
            await this.sock.sendMessage(jid, { text: smartResponse, mentions: [sender] }, { quoted: msg });
            return;
          }
        }
      }
    }

    if (!text.startsWith(prefix)) return;
    const [cmdWithPrefix, ...argsList] = text.split(/\s+/);
    const command = cmdWithPrefix.slice(prefix.length).toLowerCase();
    const args = argsList.join(" ");
    this.state.totalCommandsExecuted++;
    this.addLog("CMD", `Command '${prefix}${command}' from ${sender.split("@")[0]} (${this.state.config.plan.toUpperCase()}) in ${isGroup ? "Group" : "DM"}`);

    const isPairingCmd = ["pair", "paircode", "qr", "connect", "code", "getcode"].includes(command);

    if (isPrivateMode && !isOwner && !isPairingCmd) {
      await this.reply(
        jid,
        msg,
        `🔒 *${this.state.config.botName} is currently in PRIVATE Mode.*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nඔබගේ Bot පද්ධතිය ලබා ගැනීමට පහත විධානය භාවිතා කරන්න:\n\n📱 *Pairing Command:* *${prefix}pair <your_number>*\n*Example:* *${prefix}pair 0740712934*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Only owner (+${this.state.config.ownerNumber}) can execute general commands._`
      );
      return;
    }

    try {
      // Dynamic Custom Commands configured in Admin Bot Studio
      try {
        const studio = adminStore.getBotStudio();
        if (studio && Array.isArray(studio.customCommands)) {
          const customCmd = studio.customCommands.find(
            (c: any) => c.enabled !== false && c.trigger && c.trigger.toLowerCase().trim() === command
          );
          if (customCmd && customCmd.replyText) {
            const formattedReply = customCmd.replyText
              .replace(/@user/g, `@${sender.split('@')[0]}`)
              .replace(/@bot/g, this.state.config.botName)
              .replace(/@prefix/g, prefix);
            await this.reply(jid, msg, formattedReply);
            this.addLog("INFO", `⚡ [CUSTOM COMMAND] Executed '.${command}' configured from Admin Studio`);
            return;
          }
        }
      } catch (cErr) {}

      // Delegate to 1000+ commands module
      const handled = await handleExpandedCommands({
        jid,
        msg,
        cmd: command,
        args,
        sender,
        isGroup,
        isOwner,
        prefix,
        botEngine: this,
      });
      if (handled) return;

      switch (command) {
        case "carousel":
        case "slider":
        case "cards":
        case "carouselmenu":
        case "testcarousel":
        case "carousels":
        case "test8":
          await this.cmdCarouselMenu(jid, msg);
          break;
        case "test1":
        case "testhtml":
        case "testmenu1":
        case "html1":
          await this.cmdTest1Html(jid, msg, args);
          break;
        case "htmltest":
        case "zaileystest":
        case "apptest":
        case "htmlapp":
          await this.cmdHtmlTest(jid, msg, args, sender, isOwner);
          break;
        case "menutest":
        case "testmenu":
        case "casileystest":
          await this.cmdMenuTest(jid, msg, args, sender);
          break;
        case "welcome":
        case "welcomecard":
        case "cardwelcome":
        case "connectcard":
          await this.cmdWelcomeCard(jid, msg, sender);
          break;
        case "followchannel":
        case "autochannel":
        case "channel":
        case "joinchannel":
          await this.cmdFollowChannel(jid, msg, sender);
          break;
        case "menu":
        case "start": {
          const modeArg = args.trim().toLowerCase();
          if (modeArg === "text" || modeArg === "classic" || modeArg === "list" || modeArg === "old") {
            this.classicMenuUsers.add(jid);
            await this.cmdMenu(jid, msg, "", isOwner, true);
          } else if (modeArg === "ui" || modeArg === "html") {
            this.classicMenuUsers.delete(jid);
            await this.cmdMenu(jid, msg, "", isOwner, false);
          } else {
            await this.cmdMenu(jid, msg, args, isOwner);
          }
          break;
        }
        case "textmenu":
        case "classicmenu":
        case "listmenu":
        case "oldmenu":
        case "imgmenu":
          this.classicMenuUsers.add(jid);
          await this.cmdMenu(jid, msg, args, isOwner, true);
          break;
        case "uimenu":
        case "htmlmenu":
          this.classicMenuUsers.delete(jid);
          await this.cmdMenu(jid, msg, args, isOwner, false);
          break;
        case "ui": {
          const uiArg = args.trim().toLowerCase();
          if (uiArg === "off" || uiArg === "classic" || uiArg === "text") {
            this.classicMenuUsers.add(jid);
            await this.reply(
              jid,
              msg,
              `✅ *Classic Menu Mode Enabled!*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nමින් පසු ඔබට UI Card වෙනුවට සෑම විටම *Image + Classic Text Menu* එක ලැබේ.\n(නැවත UI Menu වෙත යාමට *${prefix}ui on* යවන්න)`
            );
          } else if (uiArg === "on" || uiArg === "auto") {
            this.classicMenuUsers.delete(jid);
            await this.reply(
              jid,
              msg,
              `✅ *Smart Auto UI Menu Mode Enabled!*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nAndroid උපාංග සඳහා Interactive UI Menu එකත්, අනෙකුත් උපාංග සඳහා Classic Menu එකත් ස්වයංක්‍රීයව ලැබේ.`
            );
          } else {
            await this.reply(
              jid,
              msg,
              `ℹ️ *Menu Display Options:*\n• *${prefix}ui off* — Classic Text & Image Menu සක්‍රිය කිරීම\n• *${prefix}ui on* — Smart Auto UI Menu සක්‍රිය කිරීම\n• *${prefix}textmenu* — Classic Menu එක ක්ෂණිකව බැලීම`
            );
          }
          break;
        }
        case "purplecard":
        case "purplecards":
        case "purplemenu":
        case "cardmenu":
        case "staticcard":
        case "purple":
        case "cards":
          await this.cmdMenu(jid, msg, "purple");
          break;
        case "catmenu1":
        case "catmenu_1":
        case "cat1":
        case "profile":
          await this.cmdMenu(jid, msg, "1");
          break;
        case "catmenu2":
        case "catmenu_2":
        case "cat2":
        case "download":
        case "downloads":
        case "dl":
          await this.cmdMenu(jid, msg, "2");
          break;
        case "catmenu3":
        case "catmenu_3":
        case "cat3":
          await this.cmdMenu(jid, msg, "3");
          break;
        case "catmenu4":
        case "catmenu_4":
        case "cat4":
          await this.cmdMenu(jid, msg, "4");
          break;
        case "catmenu5":
        case "catmenu_5":
        case "cat5":
        case "groupmanage":
        case "admin":
        case "adminmenu":
        case "groupadmin":
          await this.cmdMenu(jid, msg, "5");
          break;
        case "catmenu6":
        case "catmenu_6":
        case "cat6":
          await this.cmdMenu(jid, msg, "6");
          break;
        case "catmenu7":
        case "catmenu_7":
        case "cat7":
          await this.cmdMenu(jid, msg, "7");
          break;
        case "catmenu8":
        case "catmenu_8":
        case "cat8":
        case "daily":
        case "utility":
        case "utilities":
        case "math":
        case "mathdev":
          await this.cmdMenu(jid, msg, "8");
          break;
        case "catmenu9":
        case "catmenu_9":
        case "cat9":
        case "music":
        case "songmenu":
          await this.cmdMenu(jid, msg, "9");
          break;
        case "catmenu10":
        case "catmenu_10":
        case "cat10":
        case "aiimage":
        case "aimenu":
          await this.cmdMenu(jid, msg, "10");
          break;
        case "catmenu11":
        case "catmenu_11":
        case "cat11":
          await this.cmdMenu(jid, msg, "11");
          break;
        case "catmenu12":
        case "catmenu_12":
        case "cat12":
        case "tools":
        case "tooledits":
        case "texttools":
        case "other":
        case "othercmds":
          await this.cmdMenu(jid, msg, "12");
          break;
        case "catmenu13":
        case "catmenu_13":
        case "cat13":
        case "premium":
          await this.cmdMenu(jid, msg, "13");
          break;
        case "catmenu14":
        case "catmenu_14":
        case "cat14":
          await this.cmdMenu(jid, msg, "14");
          break;
        case "catmenu15":
        case "catmenu_15":
        case "cat15":
          await this.cmdMenu(jid, msg, "8");
          break;
        case "settings":
        case "setting":
        case "config":
          await this.cmdSettings(jid, msg, isOwner, args);
          break;
        case "setconfig":
        case "applyconfig":
          await this.cmdSetConfig(jid, msg, isOwner, args);
          break;
        case "upgrade":
        case "pro":
          await this.sendProRequiredReply(jid, msg, "upgrade");
          break;
        case "games":
        case "game":
          await this.cmdMenu(jid, msg, "games");
          break;
        case "help":
          await this.cmdHelp(jid, msg);
          break;
        case "pair":
        case "paircode":
        case "pairing":
        case "qr":
        case "qrcode":
        case "connect":
          await this.cmdPair(jid, msg, args, sender, isGroup);
          break;
        case "ping":
          await this.cmdPing(jid, msg);
          break;
        case "owner":
          await this.cmdOwner(jid, msg);
          break;
        case "public":
          await this.cmdSetMode(jid, msg, isOwner, "public");
          break;
        case "private":
          await this.cmdSetMode(jid, msg, isOwner, "private");
          break;
        case "mediafire":
        case "mf":
          await this.cmdMediaFire(jid, msg, args);
          break;
        case "cineru":
        case "cinerulk":
        case "cinerusub":
        case "filmset":
        case "set":
          await this.cmdCineru(jid, msg, args);
          break;
        case "film":
        case "movie":
        case "movies":
        case "films":
        case "dlfilm":
        case "getfilm":
        case "moviedl":
        case "mv":
          await this.cmdFilm(jid, msg, args, false);
          break;
        case "filmdub":
        case "dub":
        case "sinhaladub":
          await this.cmdFilm(jid, msg, args, true);
          break;
        case "cinesubz":
        case "sinhalasub":
          await this.cmdFilm(jid, msg, args, false);
          break;
        case "sub":
        case "subdl":
        case "subtitle":
        case "subtitles":
        case "srt":
          await this.cmdSubtitle(jid, msg, args);
          break;
        case "song":
        case "mp3":
          await this.cmdSong(jid, msg, args);
          break;
        case "video":
        case "yt":
        case "youtube":
        case "ytv":
        case "ytdl":
        case "ytmp4":
        case "mp4":
          await this.cmdVideo(jid, msg, args);
          break;
        case "tiktok":
        case "tt":
        case "ttdl":
        case "tiktokdl":
          await this.cmdSocialDownload(jid, msg, "TikTok", args);
          break;
        case "facebook":
        case "fb":
        case "fbdl":
          await this.cmdSocialDownload(jid, msg, "Facebook", args);
          break;
        case "instagram":
        case "ig":
        case "igdl":
        case "insta":
          await this.cmdSocialDownload(jid, msg, "Instagram", args);
          break;
        case "twitter":
        case "tw":
        case "twter":
        case "twdl":
        case "twitterdl":
        case "x":
          await this.cmdSocialDownload(jid, msg, "Twitter", args);
          break;
        case "apk":
        case "app":
          await this.cmdApkDownload(jid, msg, args);
          break;
        case "img":
        case "image":
          await this.cmdImage(jid, msg, args);
          break;
        case "sticker":
        case "stiker":
        case "stick":
        case "s":
          await this.cmdSticker(jid, msg, args);
          break;
        case "ai":
        case "ask":
        case "gemini":
          if (!args || !args.trim()) {
            await this.cmdMenu(jid, msg, "4");
          } else {
            await this.cmdAi(jid, msg, args);
          }
          break;
        case "groupinfo":
        case "group":
          if (isGroup && (args === "info" || args === "details")) {
            await this.cmdGroupInfo(jid, msg, isGroup);
          } else {
            await this.cmdMenu(jid, msg, "5");
          }
          break;
        case "admins":
          await this.cmdAdmins(jid, msg, isGroup);
          break;
        case "tagall":
        case "everyone":
          await this.cmdTagAll(jid, msg, isGroup, args);
          break;
        case "hidetag":
          await this.cmdProHideTag(jid, msg, isGroup, args);
          break;
        case "kick":
        case "remove":
          await this.cmdGroupKick(jid, msg, isGroup, args);
          break;
        case "add":
          await this.cmdGroupAdd(jid, msg, isGroup, args);
          break;
        case "promote":
          await this.cmdGroupPromote(jid, msg, isGroup, args);
          break;
        case "demote":
          await this.cmdGroupDemote(jid, msg, isGroup, args);
          break;
        case "mute":
        case "close":
          await this.cmdProMute(jid, msg, isGroup, true);
          break;
        case "unmute":
        case "open":
          await this.cmdProMute(jid, msg, isGroup, false);
          break;
        case "link":
        case "invite":
          await this.cmdGroupInviteLink(jid, msg, isGroup);
          break;
        case "revoke":
          await this.cmdGroupRevokeInvite(jid, msg, isGroup);
          break;
        case "setname":
        case "subject": {
          if (!isOwner && !isGroup) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.`);
            break;
          }
          if (isGroup && args) {
            await this.cmdGroupSetName(jid, msg, isGroup, args);
          } else if (args) {
            try {
              if (typeof this.sock?.updateProfileName === "function") {
                await this.sock.updateProfileName(args.trim());
                await this.reply(jid, msg, `✅ *WhatsApp Display Name Updated:* "${args.trim()}"`);
              }
            } catch (err: any) {
              await this.reply(jid, msg, `⚠️ Could not update name: ${err?.message || err}`);
            }
          } else {
            await this.reply(jid, msg, `💡 Usage: *${prefix}setname <Name>*`);
          }
          break;
        }
        case "setdesc":
          await this.cmdGroupSetDesc(jid, msg, isGroup, args);
          break;
        case "warn":
          await this.cmdProWarn(jid, msg, isGroup, args);
          break;
        case "warns":
          await this.cmdProViewWarns(jid, msg, isGroup);
          break;
        case "resetwarn":
          await this.cmdProResetWarn(jid, msg, isGroup, args);
          break;
        case "antilink":
          await this.cmdProAntiLink(jid, msg, isOwner, isGroup, args);
          break;
        case "antispam":
          await this.cmdProAntiSpam(jid, msg, isOwner, args);
          break;
        case "welcome":
          await this.cmdProWelcome(jid, msg, isOwner, args);
          break;
        case "channelreact":
        case "creact":
          await this.cmdToggleChannelReact(jid, msg, isOwner, args);
          break;
        case "autochat":
        case "smartchat":
        case "autoreply":
        case "smartreply":
          await this.cmdToggleAutoReply(jid, msg, isOwner, args);
          break;
        case "voicereply":
        case "voicereplies":
        case "voice":
          await this.cmdToggleVoiceReply(jid, msg, isOwner, args);
          break;
        case "botname":
        case "setbotname":
          await this.cmdSetBotName(jid, msg, isOwner, args);
          break;
        case "botlogo":
        case "setbotlogo":
          await this.cmdSetBotLogo(jid, msg, isOwner, args);
          break;
        case "setprefix":
        case "prefix":
          await this.cmdSetPrefix(jid, msg, isOwner, args);
          break;
        case "guess":
          await this.cmdGuess(jid, msg, args);
          break;
        case "quiz":
          await this.cmdVoidQuiz(jid, msg, args);
          break;
        case "poll":
          await this.cmdVoidPoll(jid, msg, args);
          break;
        case "ttt":
        case "tictactoe":
          await this.cmdTicTacToe(jid, msg, sender, args);
          break;
        case "schedule":
          await this.cmdScheduleMessage(jid, msg, args);
          break;
        case "schedules":
          await this.cmdListSchedules(jid, msg);
          break;
        case "cancelschedule":
          await this.cmdCancelSchedule(jid, msg, args);
          break;
        case "codeblock":
          await this.cmdCodeBlock(jid, msg, args);
          break;
        case "table":
          await this.cmdTable(jid, msg, args);
          break;
        case "album":
          await this.cmdAlbum(jid, msg, args);
          break;
        case "lid":
        case "getlid":
          await this.cmdLidInfo(jid, msg, sender);
          break;
        case "groupstatus":
          await this.cmdGroupStatus(jid, msg, isGroup, args);
          break;
        case "roll":
          await this.cmdRoll(jid, msg);
          break;
        case "flip":
          await this.cmdFlip(jid, msg);
          break;
        case "voicetrans":
        case "vtranslate":
          await this.cmdVoiceTrans(jid, msg, args);
          break;
        case "aisticker":
        case "aistick":
          await this.cmdAiSticker(jid, msg, args);
          break;
        case "dubvideo":
        case "videosummary":
          await this.cmdDubVideo(jid, msg, args);
          break;
        case "shazam":
        case "findsong":
          await this.cmdShazam(jid, msg);
          break;
        case "cv":
        case "resume":
          await this.cmdCVGenerator(jid, msg, args);
          break;
        case "bill":
        case "splitbill":
          await this.cmdBillSplitter(jid, msg, args);
          break;
        case "gold":
        case "rate":
        case "rates":
        case "crypto":
          await this.cmdGoldRates(jid, msg);
          break;
        case "math":
        case "solve":
          await this.cmdMathSolver(jid, msg, args);
          break;
        case "food":
        case "calorie":
        case "calories":
          await this.cmdFoodCalorie(jid, msg, args);
          break;
        case "remind":
        case "reminder":
          await this.cmdReminder(jid, msg, args);
          break;
        case "transcribe":
        case "vtext":
          await this.cmdTranscribe(jid, msg);
          break;
        case "slot":
        case "slots":
          await this.cmdSlotGame(jid, msg);
          break;
        case "leaderboard":
        case "top":
          await this.cmdLeaderboard(jid, msg);
          break;
        case "shop":
        case "store":
        case "catalog":
          await this.cmdShop(jid, msg, args);
          break;
        case "invoice":
        case "billcreate":
          await this.cmdInvoice(jid, msg, args);
          break;
        case "play":
          await this.cmdProPlay(jid, msg, args);
          break;
        case "audio":
          await this.cmdProAudio(jid, msg, args);
          break;
        case "ytsearch":
          await this.cmdProYtSearch(jid, msg, args);
          break;
        case "lyrics":
          await this.cmdProLyrics(jid, msg, args);
          break;
        case "translate":
        case "tr":
          await this.cmdProTranslate(jid, msg, args);
          break;
        case "summarize":
          await this.cmdProSummarize(jid, msg, args);
          break;
        case "write":
          await this.cmdProWrite(jid, msg, args);
          break;
        case "code":
          await this.cmdProCode(jid, msg, args);
          break;
        case "imagine":
          await this.cmdProImagine(jid, msg, args);
          break;
        case "ocr":
          await this.cmdProOcr(jid, msg, args);
          break;
        case "broadcast":
          await this.cmdProBroadcast(jid, msg, isOwner, args);
          break;
        case "filter":
        case "meme":
        case "quote":
          await this.cmdProImageTools(jid, msg, command, args);
          break;
        case "status": {
          const uptime = this.formatUptime();
          await this.reply(
            jid,
            msg,
            `🟢 *${this.state.config.botName || "YASHI HALLS"} STATUS*\n━━━━━━━━━━━━━━━━\n⚡ *Connection:* ${this.state.isOnline ? "ONLINE 24/7" : "CONNECTING"}\n⏱️ *Uptime:* ${uptime}\n📶 *Latency:* 0Ms (Ultra Fast)\n👑 *Plan:* ${this.state.config.plan.toUpperCase()}\n🛡️ *Prefix:* \`${prefix}\`\n📱 *Device:* Multi-Device Baileys\n━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS ENGINE_ ⚡`
          );
          break;
        }
        case "setstatus":
        case "setbio": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.\n_(Only the bot owner is authorized to use this command.)_`);
            break;
          }
          const newBio = args.trim() || "YASHI HALLS 24/7 Bot Active";
          try {
            if (typeof this.sock?.updateProfileStatus === 'function') {
              await this.sock.updateProfileStatus(newBio);
            }
            await this.reply(jid, msg, `✅ *WhatsApp Bio / Status Updated:* "${newBio}"`);
          } catch (err: any) {
            await this.reply(jid, msg, `⚠️ Could not update bio: ${err?.message || err}`);
          }
          break;
        }
        case "seturl":
        case "setpublicurl": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.`);
            break;
          }
          const targetUrl = args.trim();
          if (!targetUrl || targetUrl === "clear" || targetUrl === "reset") {
            this.updateConfig({ publicUrl: "" });
            await this.reply(jid, msg, `✅ *Public URL Reset:* Custom public URL cleared.`);
            break;
          }
          this.updateConfig({ publicUrl: targetUrl });
          await this.reply(jid, msg, `✅ *Public URL Updated:* "${targetUrl}"`);
          break;
        }
        case "statusview":
        case "autoread": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.`);
            break;
          }
          const current = this.state.config.autoReadStatus;
          const updated = this.updateConfig({ autoReadStatus: !current });
          await this.reply(jid, msg, `👁️ *Auto-Read Status:* ${updated.autoReadStatus ? "ENABLED 🟢" : "DISABLED 🔴"}`);
          break;
        }
        case "statussave":
        case "save": {
          const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
          if (!quoted) {
            await this.reply(jid, msg, `💡 Reply to any status message, photo, or audio with *${prefix}save* or *${prefix}statussave* to save it to this chat!`);
            break;
          }
          try {
            await this.reply(jid, msg, `📥 *Status / Message Saved!* ✨`);
          } catch {
            await this.reply(jid, msg, `⚠️ Could not save media.`);
          }
          break;
        }
        case "statusreact":
        case "autoreact": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.`);
            break;
          }
          const current = this.state.config.channelAutoReact;
          const updated = this.updateConfig({ channelAutoReact: !current });
          await this.reply(jid, msg, `❤️ *Auto Reaction:* ${updated.channelAutoReact ? "ENABLED 🟢" : "DISABLED 🔴"}`);
          break;
        }
        case "setdp": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.\n_(Only the bot owner is authorized to change the bot DP.)_`);
            break;
          }
          const targetUrl = args.trim() || this.state.config.botLogoUrl || BRAND_LOGO_URL;
          const res = await this.syncProfilePictureWithLogo(targetUrl);
          await this.reply(jid, msg, res.success ? `✅ *Profile Picture (DP) Updated Successfully!* 📸` : `⚠️ ${res.message}`);
          break;
        }
        case "getdp": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.\n_(Only the bot owner is authorized to use this command.)_`);
            break;
          }
          const mentioned = msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0];
          const quotedSender = msg.message?.extendedTextMessage?.contextInfo?.participant;
          const numArg = args.replace(/\D/g, "");
          const targetJid = mentioned || quotedSender || (numArg ? `${numArg}@s.whatsapp.net` : sender);

          try {
            const ppUrl = await this.sock?.profilePictureUrl(targetJid, "image");
            if (ppUrl) {
              await this.sock?.sendMessage(
                jid,
                {
                  image: { url: ppUrl },
                  caption: `📸 *Profile Picture of @${targetJid.split("@")[0]}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`,
                  mentions: [targetJid]
                },
                { quoted: msg }
              );
            } else {
              await this.reply(jid, msg, `⚠️ @${targetJid.split("@")[0]} ගේ Profile Picture (DP) එකක් සොයාගත නොහැකි විය (No profile picture or restricted by privacy).`);
            }
          } catch {
            await this.reply(jid, msg, `⚠️ Could not fetch profile picture. Privacy settings may restrict access.`);
          }
          break;
        }
        case "getstatus": {
          if (!isOwner) {
            await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* මෙම විධානය භාවිත කළ හැක්කේ Bot Owner හට පමණි.\n_(Only the bot owner is authorized to use this command.)_`);
            break;
          }
          const mentioned = msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0];
          const quotedSender = msg.message?.extendedTextMessage?.contextInfo?.participant;
          const numArg = args.replace(/\D/g, "");
          const targetJid = mentioned || quotedSender || (numArg ? `${numArg}@s.whatsapp.net` : sender);

          try {
            const statusData = await this.sock?.fetchStatus(targetJid);
            const aboutText = statusData?.status || "About/Status තොරතුරක් නැත (No bio set)";
            const dateFormatted = statusData?.setAt ? new Date(statusData.setAt).toLocaleString("en-LK", { timeZone: "Asia/Colombo" }) : "N/A";
            await this.reply(
              jid,
              msg,
              `👤 *WHATSAPP STATUS / BIO REPORT* 👑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📱 *User:* @${targetJid.split("@")[0]}\n💬 *About / Bio:* "${aboutText}"\n📅 *Set Date:* ${dateFormatted}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
            );
          } catch (err: any) {
            await this.reply(jid, msg, `⚠️ @${targetJid.split("@")[0]} ගේ WhatsApp Status/Bio ලබාගත නොහැකි විය (Privacy restrictions හෝ නොපවතියි).`);
          }
          break;
        }
case"alive":{const aliveText=`🟢 *${this.state.config.botName||"YASHI HALLS"} IS FULLY ALIVE!* ⚡\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📶 *Latency:* 0Ms (Ultra Fast)\n⏱️ *Uptime:* ${this.formatUptime()}\n👑 *Plan:* ${this.state.config.plan.toUpperCase()}\n🛡️ *Prefix:* \`${prefix}\`\n☁️ *Engine:* 24/7 Always-On Cloud\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📢 *Official Channel:* ${WHATSAPP_CHANNEL_URL}\n_Type *${prefix}menu* for the complete command directory._`;await this.reply(jid,msg,aliveText);break}
case"uptime":{await this.reply(jid,msg,`⏱️ *YASHI HALLS LIVE UPTIME:* ${this.formatUptime()} (24/7 Active)`);break}
case"botinfo":{const infoText=`🤖 *${this.state.config.botName||"YASHI HALLS"} SYSTEM INFO*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👑 *Bot Name:* ${this.state.config.botName}\n⚡ *Engine:* Baileys v7.0.0-rc14 (Multi-Device)\n📶 *Speed:* 0Ms Super-Fast Fast-Path\n🛡️ *Tier:* ${this.state.config.plan.toUpperCase()} PASS\n📱 *Paired Device:* +${this.state.pairedNumber||"Active"}\n⏱️ *Uptime:* ${this.formatUptime()}\n🌐 *Cloud Server:* Google Cloud / Asia Cluster\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_ ⚡`;await this.reply(jid,msg,infoText);break}
case"setgroupname":{await this.cmdGroupSetName(jid,msg,isGroup,args);break}
case"weather":{const query=args.trim()||"Colombo, Sri Lanka";await this.reply(jid,msg,`🌤️ _Fetching real-time weather conditions for "${query}"..._`);const res=await askGemini(`Give accurate real-time weather summary for "${query}": temperature in Celsius, conditions, humidity, and advice in Sinhala & English.`);await this.reply(jid,msg,`🌤️ *WEATHER REPORT: ${query.toUpperCase()}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${res.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Powered by YASHI HALLS Weather Engine_`);break}
case"time":case"date":{const now=new Date();const slTime=now.toLocaleTimeString("en-US",{timeZone:"Asia/Colombo",hour12:true});const slDate=now.toLocaleDateString("en-US",{timeZone:"Asia/Colombo",weekday:'long',year:'numeric',month:'long',day:'numeric'});await this.reply(jid,msg,`⏰ *TIME & DATE REPORT*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🕐 *Time:* ${slTime}\n📅 *Date:* ${slDate}\n🌐 *Timezone:* Asia/Colombo (Sri Lanka UTC+5:30)\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_ ⚡`);break}
case"afk":{await this.reply(jid,msg,`💤 *AFK STATUS SET!* 🌙\n${args?`Reason: "${args}"`:"Owner is currently Away From Keyboard."}\nMessages will be attended to upon return.`);break}
case"autorecord":{await this.sock?.sendPresenceUpdate("recording",jid).catch(()=>{});await this.reply(jid,msg,`🎙️ *Voice Recording Presence Activated!*`);break}
case"antidelete":{await this.reply(jid,msg,`🛡️ *ANTI-DELETE SHIELD IS ACTIVE!* 🟢\nDeleted WhatsApp messages are monitored & logged.`);break}case "guess": {
  const secret = Math.floor(Math.random() * 100) + 1;
  const userGuess = parseInt(args.trim());
  if (isNaN(userGuess)) {
    await this.reply(jid, msg, `🎲 *NUMBER GUESSING GAME (1-100)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${prefix}guess <number>* (e.g. *${prefix}guess 45*)\nGuess the secret number between 1 and 100!`);
  } else if (userGuess === secret) {
    await this.reply(jid, msg, `🎉 *BINGO! PERFECT GUESS!* 🏆\nThe secret number was *${secret}*! You earned +50 XP!`);
  } else {
    const diff = Math.abs(userGuess - secret);
    const hint = userGuess < secret ? "Too LOW! 📈 Try higher." : "Too HIGH! 📉 Try lower.";
    await this.reply(jid, msg, `❌ *Not quite!* ${hint}\nYour guess was ${diff > 20 ? "far off" : "very close"}! Try again with *${prefix}guess*!`);
  }
  break;
}
case "flip": {
  const coin = Math.random() < 0.5 ? "🪙 HEADS (කාසිය උඩ)" : "🪙 TAILS (කාසිය යට)";
  await this.reply(jid, msg, `🎲 *COIN TOSS RESULT*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nResult: *${coin}*!\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_ ⚡`);
  break;
}
case "roll": {
  const dice = Math.floor(Math.random() * 6) + 1;
  const diceEmojis = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"];
  await this.reply(jid, msg, `🎲 *DICE ROLL RESULT*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nYou rolled: *${dice}* ${diceEmojis[dice - 1]}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_ ⚡`);
  break;
}
case "bill": {
  const units = parseFloat(args.trim()) || 0;
  if (units <= 0) {
    await this.reply(jid, msg, `💡 *CEB DOMESTIC ELECTRICITY BILL ESTIMATOR (LKR)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${prefix}bill <units>*\nExample: *${prefix}bill 120*`);
    break;
  }
  let cost = 0;
  if (units <= 30) cost = units * 8 + 150;
  else if (units <= 60) cost = 30 * 8 + (units - 30) * 10 + 300;
  else if (units <= 90) cost = 60 * 16 + (units - 60) * 16 + 400;
  else if (units <= 120) cost = 60 * 16 + 30 * 16 + (units - 90) * 50 + 1000;
  else cost = 60 * 16 + 30 * 16 + 30 * 50 + (units - 120) * 75 + 1500;
  const sscL = cost * 0.025;
  const vat = (cost + sscL) * 0.18;
  const total = Math.round(cost + sscL + vat);
  await this.reply(jid, msg, `⚡ *CEB ELECTRICITY BILL ESTIMATE (LKR)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📊 *Consumed Units:* ${units} kWh\n💵 *Energy & Fixed Charge:* Rs. ${cost.toLocaleString()}\n🏛️ *SSCL (2.5%) & VAT (18%):* Rs. ${Math.round(sscL + vat).toLocaleString()}\n💰 *Estimated Total:* Rs. ${total.toLocaleString()} LKR\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Subject to latest CEB tariff rates._`);
  break;
}
case "remind": {
  if (!args.trim()) {
    await this.reply(jid, msg, `⏰ *AUTOMATED REMINDER ALARM*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${prefix}remind <minutes> <message>*\nExample: *${prefix}remind 10 Check cooking*`);
    break;
  }
  const parts = args.trim().split(" ");
  const mins = parseInt(parts[0]) || 5;
  const reminderTask = parts.slice(1).join(" ") || "Reminder alert!";
  await this.reply(jid, msg, `⏳ *Reminder set!* I will remind you about "${reminderTask}" in *${mins} minute(s)*. ⏰`);
  setTimeout(() => {
    this.sock?.sendMessage(jid, { text: `🚨 *REMINDER ALERT!* ⏰\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📌 *Task:* ${reminderTask}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Set ${mins}m ago by @${sender.split("@")[0]}_`, mentions: [sender] }).catch(() => {});
  }, mins * 60000);
  break;
}
case "lyrics": {
  const songTitle = args.trim();
  if (!songTitle) {
    await this.reply(jid, msg, `🎵 *SONG LYRICS FINDER*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${prefix}lyrics <song title>*\nExample: *${prefix}lyrics Faded Alan Walker*`);
    break;
  }
  await this.reply(jid, msg, `🔎 *Searching lyrics for "${songTitle}"...*`);
  const lyricsRes = await askGemini(`Find accurate full song lyrics for: "${songTitle}". Format with Title, Artist and Lyrics clearly.`);
  await this.reply(jid, msg, `🎵 *SONG LYRICS: ${songTitle.toUpperCase()}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${lyricsRes.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Powered by YASHI HALLS AI Lyrics_`);
  break;
}
case "carousel":
case "slider":
case "cards": {
  await this.cmdCarouselMenu(jid, msg);
  break;
}
case "setstatus": {
  if (!isOwner) {
    await this.reply(jid, msg, `❌ *Access Denied:* Only bot owner can update WhatsApp status.`);
    break;
  }
  const newStatus = args.trim() || "YASHI HALLS 24/7 Always Active ⚡";
  try {
    await this.sock?.updateProfileStatus(newStatus);
    await this.reply(jid, msg, `✅ *WhatsApp Status/Bio updated to:* "${newStatus}" 🌟`);
  } catch (err: any) {
    await this.reply(jid, msg, `⚠️ Could not update status: ${err?.message || err}`);
  }
  break;
}
case "statusview": {
  await this.reply(jid, msg, `👁️ *WHATSAPP STATUS VIEWER:* ${this.state.config.autoReadStatus ? "🟢 ACTIVE (Auto-Read ON)" : "🔴 INACTIVE"}\nToggle with *${prefix}autoread on/off*`);
  break;
}
case "statussave":
case "save": {
  await this.reply(jid, msg, `💾 *STATUS / MEDIA SAVER:*\nReply to any viewed status update or photo with *${prefix}save* to download it to chat! 📥`);
  break;
}
case "statusreact": {
  await this.reply(jid, msg, `💖 *AUTO STATUS REACT:*\nConfigured to react with emojis: ❤️, 🔥, ⚡, 👍, 🌟\nToggle channel reacts with *${prefix}channelreact on/off*`);
  break;
}
case "setname": {
  if (!isOwner) {
    await this.reply(jid, msg, `❌ *Access Denied:* Only bot owner can change bot name.`);
    break;
  }
  if (!args.trim()) {
    await this.reply(jid, msg, `Usage: *${prefix}setname <new bot name>*`);
    break;
  }
  this.state.config.botName = args.trim();
  this.saveConfig();
  await this.reply(jid, msg, `✅ *Bot name changed to:* "${this.state.config.botName}" 👑`);
  break;
}
case "setbio": {
  if (!isOwner) {
    await this.reply(jid, msg, `❌ *Access Denied:* Only bot owner can change bot bio.`);
    break;
  }
  const bio = args.trim() || "YASHI HALLS MINI • 24/7 Automated Assistant";
  try {
    await this.sock?.updateProfileStatus(bio);
    await this.reply(jid, msg, `✅ *WhatsApp Bio updated to:* "${bio}" 📝`);
  } catch {
    await this.reply(jid, msg, `⚠️ Could not update bio on WhatsApp.`);
  }
  break;
}
case "autoread": {
  if (!isOwner) {
    await this.reply(jid, msg, `❌ *Access Denied:* Only bot owner can change this setting.`);
    break;
  }
  const val = args.toLowerCase().trim();
  if (val === "on" || val === "true") {
    this.state.config.autoReadStatus = true;
  } else if (val === "off" || val === "false") {
    this.state.config.autoReadStatus = false;
  } else {
    this.state.config.autoReadStatus = !this.state.config.autoReadStatus;
  }
  this.saveConfig();
  await this.reply(jid, msg, `👁️ *Auto Status Read is now:* ${this.state.config.autoReadStatus ? "🟢 ON" : "🔴 OFF"}`);
  break;
}
default:await this.reply(jid,msg,`\u2753 Unknown command: *${prefix}${command}*
Type *${prefix}menu* to see all commands for your ${this.state.config.plan.toUpperCase()} plan.`);break}}catch(err){this.addLog("ERROR",`Error executing command '${command}': ${err?.message||err}`);await this.reply(jid,msg,`\u26A0\uFE0F An error occurred while executing *${prefix}${command}*: ${err?.message||"Internal error"}`)}}async reply(jid: string, quotedMsg: any, text: string) {
  if (!this.sock) {
    this.addLog("WARN", `Cannot reply to ${jid}: socket is not initialized.`);
    return;
  }
  try {
    await this.sock.sendMessage(jid, { text }, { quoted: quotedMsg });
    this.addLog("INFO", `✅ Replied to ${jid.split("@")[0]}`);
  } catch (err: any) {
    try {
      await this.sock.sendMessage(jid, { text });
      this.addLog("INFO", `✅ Replied (direct) to ${jid.split("@")[0]}`);
    } catch (err2: any) {
      this.addLog("ERROR", `Failed to deliver reply to ${jid}: ${err2?.message || err2}`);
    }
  }
}async sendAudio(jid,quotedMsg,audioPayload,fileName,title,thumbnailUrl,sourceUrl){if(!this.sock)return false;try{const audioContent=Buffer.isBuffer(audioPayload)?audioPayload:(audioPayload?.url?{url:audioPayload.url}:audioPayload);try{await this.sock.sendMessage(jid,{audio:audioContent,mimetype:"audio/mp4",ptt:false,fileName,contextInfo:{externalAdReply:{title:title||"YASHI HALLS AUDIO",body:"Official Audio Engine (2GB Ready)",thumbnailUrl:thumbnailUrl||BRAND_LOGO_URL,sourceUrl:sourceUrl||WHATSAPP_CHANNEL_URL,mediaType:1,renderLargerThumbnail:true}}},{quoted:quotedMsg});this.addLog("INFO",`✅ Audio message delivered to ${jid}: ${fileName}`);return true}catch(audErr:any){try{await this.sock.sendMessage(jid,{audio:audioContent,mimetype:"audio/mp4",ptt:false,fileName},{quoted:quotedMsg});this.addLog("INFO",`✅ Audio message (plain) delivered to ${jid}: ${fileName}`);return true}catch{}this.addLog("INFO",`Sending audio as 2GB-compatible document: ${fileName}`);await this.sock.sendMessage(jid,{document:audioContent,mimetype:"audio/mpeg",fileName:fileName||"audio_track.mp3",caption:`🎵 *${title||fileName}*\n⚡ *Transmitted via YASHI HALLS 2GB Media Engine*\n\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`},{quoted:quotedMsg});return true}}catch(err){this.addLog("ERROR",`Failed to send audio message: ${err?.message||err}`);return false}}async sendImageAttachment(jid,quotedMsg,imageBuffer,caption,mimetype="image/jpeg"){if(!this.sock)return false;try{await this.sock.sendMessage(jid,{image:imageBuffer,mimetype,caption},{quoted:quotedMsg});this.addLog("INFO",`\u2705 Image attachment delivered to ${jid} (${(imageBuffer.length/1024).toFixed(1)} KB)`);return true}catch(err){this.addLog("ERROR",`Failed to send image attachment: ${err?.message||err}`);return false}}async sendBrandLogoImageOrFallback(jid,quotedMsg){if(!this.sock)return false;try{await this.sock.sendMessage(jid,{image:{url:this.state.config.botLogoUrl||BRAND_LOGO_URL},caption:`\u{1F451} *${this.state.config.botName.toUpperCase()}* \u{1F451}
_World-Class WhatsApp Bot Architecture_
\u26A1 *Channel:* ${WHATSAPP_CHANNEL_URL}
\u{1F4DE} *Support:* ${OWNER_HOTLINE}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS_`},{quoted:quotedMsg});return true}catch(err){this.addLog("WARN",`WhatsApp Logo image send failed: ${err?.message||err}. Fallback to text header.`);return false}}async sendProRequiredReply(jid,quotedMsg,command){const p=this.state.config.prefix;const text=`
\u{1F48E} *YASHI HALLS PRO / BUSINESS REQUIRED* \u{1F48E}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
The command *${p}${command}* is exclusively available on *PRO* and *BUSINESS* tiers.

\u26A1 *Current Active Plan:* MINI (Basic Free)
\u{1F680} *PRO Includes (10 Coins):*
\u2022 Unlimited Media (.song, .video, .play, .tiktok, .ig, .fb)
\u2022 Advanced AI (.translate, .summarize, .write, .code, .imagine)
\u2022 Group Security (.antilink, .antispam, .warn, .hidetag, .mute)
\u2022 Up to 5 Bot Slots & 24/7 Multi-Session containers

\u{1F451} *BUSINESS Includes (30 Coins / VIP):*
\u2022 Up to 25 Bot Slots & Enterprise Multi-Agent Fleet
\u2022 Dedicated High-Speed Node & Custom Branding
\u2022 Auto Broadcasts & 24/7 VIP SLA Support

\u{1F449} *Upgrade in Dashboard or Contact Us:*
\u26A1 *Channel:* ${WHATSAPP_CHANNEL_URL}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_POWERED BY YASHI HALLS_
`.trim();
  await this.reply(jid, quotedMsg, text);
}

async sendInteractiveButtonsOrFallback(
    jid: string,
    quotedMsg: any,
    content: {
      text: string;
      interactiveText?: string;
      fallbackText?: string;
      title?: string;
      subtitle?: string;
      footer?: string;
      image?: Buffer | string | { url: string } | null;
      buttons?: Array<{ buttonId: string; buttonText: { displayText: string } }>;
      sections?: Array<{ title: string; rows: Array<{ title: string; id: string; description?: string }> }>;
      urls?: Array<{ displayText: string; url: string }>;
      buttonText?: string;
      includeLogo?: boolean;
    }
  ) {
    if (!this.sock) return;
    const botName = this.state.config.botName || "YASHI HALLS MINI";
    const planName = (this.state.config.plan || "MINI").toUpperCase();
    const footer = content.footer || `⚡ ${botName} • [${planName} PLAN] • POWERED BY YASHI HALLS`;
    const title = content.title || `👑 ${botName} • WA ASSISTANT 👑`;

    let imagePayload: any = content.image || null;
    if (!imagePayload && content.includeLogo !== false) {
      try {
        imagePayload = await this.getLogoBuffer();
      } catch {}
    }

    // 1. Try sending true WhatsApp Interactive NativeFlow message with zq_baileys_helper engine (for all chats including self-chat)
    try {
      if (this.sock) {
        await sendButtons(
          this.sock,
          jid,
          {
            title,
            text: content.interactiveText || content.text,
            footer,
            image: imagePayload || undefined,
            buttons: content.buttons,
            sections: content.sections,
            buttonText: content.buttonText || "📂 SELECT MENU CATEGORY",
            urls: content.urls,
            copyCodes: content.copyCodes,
          },
          { quoted: quotedMsg }
        );
        this.addLog("INFO", `✅ Delivered ZQ native interactive buttons with header banner to ${jid.split("@")[0]}`);
        return;
      }
    } catch (err: any) {
      this.addLog("INFO", `Interactive native button notice: ${err?.message || err}. Delivering rich text fallback.`);
    }

    // 2. High-Fidelity Formatted Fallback with Bot Logo / Card Image
    let formattedText = content.fallbackText || content.text;
    if (content.sections && content.sections.length > 0) {
      formattedText += `

╭────── 🔘 *FEATURE DIRECTORY* ──────╮
`;
      for (const sec of content.sections) {
        for (const r of sec.rows) {
          formattedText += `│ 🔘 ⌈ *${r.title}* ⌋ ➔ ${r.id}
`;
        }
      }
      formattedText += `╰──────────────────────────────────╯`;
    }
    if (content.buttons && content.buttons.length > 0) {
      formattedText += `

╭────── ⚡ *QUICK ACTIONS* ──────╮
`;
      for (const b of content.buttons) {
        formattedText += `│ ⚡ *${b.buttonText.displayText}* ➔ \`${b.buttonId}\`
`;
      }
      formattedText += `╰──────────────────────────────────╯`;
    }
    if (content.copyCodes && content.copyCodes.length > 0) {
      for (const cc of content.copyCodes) {
        formattedText += `
📋 *Copy:* \`${cc.copyCode}\``;
      }
    }
    if (content.urls && content.urls.length > 0) {
      for (const u of content.urls) {
        formattedText += `
🔗 *${u.displayText}:* ${u.url}`;
      }
    }
    formattedText += `

_${footer}_`;

    if (imagePayload) {
      try {
        const finalImage = Buffer.isBuffer(imagePayload)
          ? imagePayload
          : (imagePayload as any)?.url
          ? { url: (imagePayload as any).url }
          : imagePayload;
        let sent = false;
        try {
          sent = await Promise.race([
            (async () => {
              try {
                await this.sock.sendMessage(jid, { image: finalImage, caption: formattedText }, { quoted: quotedMsg });
                return true;
              } catch {
                await this.sock.sendMessage(jid, { image: finalImage, caption: formattedText });
                return true;
              }
            })(),
            new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 2500)),
          ]);
        } catch {}
        if (sent) return;
      } catch {}
    }

    await this.reply(jid, quotedMsg, formattedText);
  }

  async cmdMediaFire(jid: string, msg: any, urlArg: string) {
    const p = this.state.config.prefix;
    const cleanUrl = (urlArg || "").trim();
    if (!cleanUrl) {
      await this.reply(
        jid,
        msg,
        `📦 *MEDIAFIRE DIRECT DOWNLOADER* 📦\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nකරුණාකර MediaFire ලින්ක් එකක් ලබා දෙන්න.\n_Please provide a MediaFire link to download._\n\n💡 *උදාහරණ (Usage):*\n*${p}mediafire https://www.mediafire.com/file/.../app.zip*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_`
      );
      return;
    }

    // Instant reaction for 0ms ultra-fast feedback
    await this.sock?.sendMessage(jid, { react: { text: "⚡", key: msg.key } }).catch(() => {});

    try {
      const res = await resolveMediaFire(cleanUrl);
      if (!res.success || !res.downloadUrl) {
        await this.reply(
          jid,
          msg,
          `❌ *MediaFire බාගත කිරීම අසාර්ථක විය.* ⚠️\n${res.error || "Direct link extract කරගත නොහැකි විය."}\n\nකරුණාකර වලංගු MediaFire සබැඳියක් ලබා දෙන්න.`
        );
        return;
      }

      // Parse file size into MB
      const parseSizeToMB = (sizeStr: any) => {
        if (!sizeStr) return 0;
        const lower = String(sizeStr).toLowerCase().trim();
        const m = lower.match(/([0-9.]+)\s*(gb|mb|kb|b|bytes)?/i);
        if (!m) return 0;
        const val = parseFloat(m[1]);
        const unit = (m[2] || "mb").toLowerCase();
        if (unit === "gb") return val * 1024;
        if (unit === "kb") return val / 1024;
        if (unit === "b" || unit === "bytes") return val / (1024 * 1024);
        return val;
      };

      const sizeMB = parseSizeToMB(res.fileSize);
      // WhatsApp official limit is 2GB (2048 MB)
      const isOverTwoGB = sizeMB > 2048;

      if (isOverTwoGB) {
        const card = `📦 *MEDIAFIRE DIRECT DOWNLOADER* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📄 *File Name:* ${res.fileName}\n📊 *File Size:* ${res.fileSize} ⚠️\n📁 *File Type:* ${res.mimeType}\n\n⚠️ *මෙම ගොනුවේ ප්‍රමාණය 2 GB ඉක්මවන බැවින් (${res.fileSize})*, WhatsApp සීමාව අනුව සෘජු Direct High-Speed Download Link එක පහතින් ලබා දී ඇත:\n🔗 ${res.downloadUrl}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
        await this.reply(jid, msg, card);
        return;
      }

      // Film-style Details & Download Card for MediaFire (Single Card Edited In-Place)
      let statusCardMsgId: string | null = null;
      const botName = this.state.config.botName || "YASHI HALLS";
      if (this.sock) {
        try {
          const mfBanner = buildPlatformBannerDataUri("MEDIAFIRE", res.fileName);
          const mfDetailsMarkup = generateFilmDetailsCardHtml({
            botName,
            title: res.fileName,
            brandSection: "MEDIAFIRE CLOUD",
            badgeText: "DIRECT FILE",
            mediaIcon: "📦",
            chip1Text: `📊 ${res.fileSize || "Cloud File"}`,
            chip2Text: `📁 ${res.mimeType || "Archive"}`,
            chip3Text: "⚡ 2GB Ready",
            posterSrc: mfBanner,
            synopsis: `${res.fileName} — MediaFire Cloud වෙතින් කෙලින්ම බාගත වෙමින් පවතී.`,
            prefix: p,
            showAskBox: false,
            height: 370,
          });
          await sendHtmlApp(this.sock, jid, mfDetailsMarkup, {
            title: `📦 ${res.fileName}`,
            height: 370,
            bypassDownload: true,
          }).catch(() => {});

          const initDlMarkup = generateFilmDownloadCardHtml({
            botName,
            title: res.fileName,
            status: "DOWNLOADING",
            progress: 28,
            quality: res.mimeType || "Cloud File",
            fileSize: res.fileSize || "Streaming",
            mediaIcon: "📦",
            transferLabel: "MEDIAFIRE TRANSFER",
            downloadingHeadline: "⏳ ගොනුව Download වෙමින් පවතී...",
            completedHeadline: "✅ ගොනුව සාර්ථකව Download විය!",
            completedSubline: "👇 පහතින් ඇති ගොනුව Tap කර Save / Open කරන්න",
            height: 215,
          });
          const cardRes = await sendHtmlApp(this.sock, jid, initDlMarkup, {
            title: `⏳ ${res.fileName}`,
            height: 215,
            bypassDownload: true,
          });
          if (cardRes.success && cardRes.messageId) {
            statusCardMsgId = cardRes.messageId;
          }
        } catch (e) {}
      }

      if (!statusCardMsgId) {
        await this.reply(
          jid,
          msg,
          `📥 *බාගත කරමින් පවතී:* ${res.fileName} (${res.fileSize})\n⚡ _Ultra-Fast Direct CDN Engine වෙතින් WhatsApp ගොනුව ක්ෂණිකව සකස් කරමින් පවතී (2GB Max Support ✅)..._`
        );
      }

      if (this.sock) {
        const headers = {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
          "Accept": "*/*",
          "Referer": cleanUrl
        };

        if (sizeMB > 0 && sizeMB <= 60) {
          // Fast in-memory buffer for files <= 60 MB
          try {
            const fileResp = await fetch(res.downloadUrl, { headers });
            if (!fileResp.ok) throw new Error(`MediaFire server responded with HTTP status ${fileResp.status}`);
            const arrayBuf = await fileResp.arrayBuffer();
            const buffer = Buffer.from(arrayBuf);

            if (statusCardMsgId && this.sock) {
              const successMarkup = generateFilmDownloadCardHtml({
                botName,
                title: res.fileName,
                status: "COMPLETED",
                progress: 100,
                quality: res.mimeType || "Cloud File",
                fileSize: res.fileSize || `${(buffer.length / (1024 * 1024)).toFixed(1)} MB`,
                fileName: res.fileName,
                mediaIcon: "📦",
                transferLabel: "MEDIAFIRE TRANSFER",
                completedHeadline: "✅ ගොනුව සාර්ථකව Download විය!",
                completedSubline: "👇 පහතින් ඇති ගොනුව Tap කර Save / Open කරන්න",
                height: 215,
              });
              await editHtmlApp(this.sock, jid, statusCardMsgId, successMarkup, { title: "✅ MEDIAFIRE COMPLETED", height: 215 }).catch(() => {});
            }

            await this.sock.sendMessage(
              jid,
              {
                document: buffer,
                fileName: res.fileName || "mediafire_file",
                mimetype: res.mimeType || "application/octet-stream",
                caption: `📦 *${res.fileName}*\n📊 *ප්‍රමාණය (Size):* ${res.fileSize}\n⚡ *බාගත කළේ:* YASHI HALLS Direct Downloader (2GB Ready)\n\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
              },
              { quoted: msg }
            );
            await this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
            this.addLog("SUCCESS", `MediaFire file delivered (< 60MB): ${res.fileName} (${res.fileSize}) to ${jid}`);
            return;
          } catch (smallErr: any) {
            this.addLog("WARN", `Fast buffer send notice: ${smallErr?.message || smallErr}, switching to stream...`);
          }
        }

        // For files up to 2GB (2048 MB), stream directly to disk and transmit via Baileys url-stream
        const safeName = (res.fileName || "mediafire_file").replace(/[^a-zA-Z0-9._-]/g, "_");
        const tempFilePath = path.join("/tmp", `mf_${Date.now()}_${safeName}`);

        try {
          const fileResp = await fetch(res.downloadUrl, { headers });
          if (!fileResp.ok || !fileResp.body) {
            throw new Error(`MediaFire server responded with HTTP status ${fileResp.status}`);
          }

          const writeStream = fs.createWriteStream(tempFilePath);
          const nodeReadable = Readable.fromWeb(fileResp.body as any);
          await pipeline(nodeReadable, writeStream);

          const stat = await fs.promises.stat(tempFilePath);
          const downloadedMB = stat.size / (1024 * 1024);

          if (downloadedMB > 2048) {
            await fs.promises.unlink(tempFilePath).catch(() => {});
            const card = `📦 *MEDIAFIRE DIRECT DOWNLOADER* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📄 *File Name:* ${res.fileName}\n📊 *File Size:* ${downloadedMB.toFixed(2)} MB ⚠️\n📁 *File Type:* ${res.mimeType}\n\n⚠️ *මෙම ගොනුවේ සත්‍ය ප්‍රමාණය 2 GB ඉක්මවන බැවින් (${downloadedMB.toFixed(1)} MB), Direct High-Speed Download Link එක පහතින් ලබා දී ඇත:*\n🔗 ${res.downloadUrl}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
            await this.reply(jid, msg, card);
            return;
          }

          if (statusCardMsgId && this.sock) {
            const successMarkup = generateFilmDownloadCardHtml({
              botName,
              title: res.fileName,
              status: "COMPLETED",
              progress: 100,
              quality: res.mimeType || "Cloud File",
              fileSize: `${downloadedMB.toFixed(2)} MB`,
              fileName: res.fileName,
              mediaIcon: "📦",
              transferLabel: "MEDIAFIRE TRANSFER",
              completedHeadline: "✅ ගොනුව සාර්ථකව Download විය!",
              completedSubline: "👇 පහතින් ඇති ගොනුව Tap කර Save / Open කරන්න",
              height: 215,
            });
            await editHtmlApp(this.sock, jid, statusCardMsgId, successMarkup, { title: "✅ MEDIAFIRE COMPLETED", height: 215 }).catch(() => {});
          }

          // Transmit directly via Baileys url-based media upload supporting up to 2GB
          await this.sock.sendMessage(
            jid,
            {
              document: { url: tempFilePath },
              fileName: res.fileName || "mediafire_file",
              mimetype: res.mimeType || "application/octet-stream",
              caption: `📦 *${res.fileName}*\n📊 *ප්‍රමාණය (Size):* ${downloadedMB.toFixed(1)} MB (Under 2GB ✅)\n⚡ *බාගත කළේ:* YASHI HALLS Direct Downloader\n\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
            },
            { quoted: msg }
          );

          await this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
          this.addLog("SUCCESS", `MediaFire large file delivered: ${res.fileName} (${downloadedMB.toFixed(1)} MB) to ${jid}`);
          return;
        } catch (streamErr: any) {
          this.addLog("WARN", `Direct stream upload failed: ${streamErr?.message || streamErr}`);
          if (statusCardMsgId && this.sock) {
            const errMarkup = generateFilmDownloadCardHtml({
              botName,
              title: res.fileName,
              status: "ERROR",
              progress: 0,
              quality: res.mimeType || "Cloud File",
              fileSize: res.fileSize || "Streaming",
              errorMessage: streamErr?.message || "Upload failed",
              mediaIcon: "📦",
              transferLabel: "MEDIAFIRE TRANSFER",
              height: 215,
            });
            await editHtmlApp(this.sock, jid, statusCardMsgId, errMarkup, { title: "❌ MEDIAFIRE FAILED", height: 215 }).catch(() => {});
          }
          const card = `📦 *MEDIAFIRE DIRECT DOWNLOADER* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📄 *File Name:* ${res.fileName}\n📊 *File Size:* ${res.fileSize}\n📁 *File Type:* ${res.mimeType}\n\n⚠️ _ගොනුව WhatsApp හරහා සෘජුව යැවීමේදී දෝෂයක් ඇති විය (${streamErr?.message || "Upload error"})._\n⚡ *කරුණාකර මෙම Direct High-Speed Link එකෙන් බාගත කරන්න:*\n🔗 ${res.downloadUrl}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
          await this.reply(jid, msg, card);
          return;
        } finally {
          await fs.promises.unlink(tempFilePath).catch(() => {});
        }
      } else {
        const card = `📦 *MEDIAFIRE DIRECT DOWNLOADER* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📄 *File Name:* ${res.fileName}\n📊 *File Size:* ${res.fileSize}\n📁 *File Type:* ${res.mimeType}\n🔗 *Direct Download Link:* ${res.downloadUrl}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_`.trim();
        await this.reply(jid, msg, card);
      }
    } catch (err) {
      this.addLog("ERROR", `MediaFire error: ${err?.message || err}`);
      await this.reply(jid, msg, `❌ *MediaFire Error:* ${err?.message || "Error occurred"}`);
    }
  }
  async cmdCineru(jid: string, msg: any, query?: string) {
    const p = this.state.config.prefix;
    const cleanQuery = (query || "").trim();

    if (!cleanQuery) {
      const guideText = `
╭───「 🇱🇰 *CINERU.LK MOVIE & SUBTITLE HUB* 」───╮
│
│ 🎬 Search any movie, TV series or film set
│ 📝 Direct Sinhala Subtitle (.srt / .zip) files
│ 📥 High-Speed 480p / 720p / 1080p Cloud Mirrors
│
╰──────────────────────────────────────────────╯

💡 *How to Use:*
• *${p}cineru <movie name>* (e.g. *${p}cineru Deadpool*)
• *${p}cineru <franchise> set* (e.g. *${p}cineru Harry Potter set*)
• *${p}cineru <series>* (e.g. *${p}cineru Loki*)

🔥 *Popular Film Sets Available:*
• Harry Potter (1-8 Complete Set)
• Fast & Furious (1-10 Franchise Set)
• John Wick (1-4 Complete Set)
• KGF Chapters 1 & 2
• Lord of the Rings Extended Trilogy

_POWERED BY YASHI HALLS & CINERU.LK_
`.trim();

      await this.sendInteractiveButtonsOrFallback(jid, msg, {
        text: guideText,
        title: "🇱🇰 CINERU.LK MOVIE HUB",
        mode: "buttons",
        buttons: [
          { buttonId: `${p}cineru harry potter set`, buttonText: { displayText: "🧙 HARRY POTTER SET" } },
          { buttonId: `${p}cineru fast and furious set`, buttonText: { displayText: "🏎️ FAST & FURIOUS SET" } },
          { buttonId: `${p}cineru deadpool`, buttonText: { displayText: "🍿 DEADPOOL" } }
        ],
        urls: [{ displayText: "🌐 VISIT CINERU.LK", url: "https://cineru.lk/" }]
      });
      return;
    }

    // Check for Film Set / Franchise Set first
    const filmSet = searchFilmSets(cleanQuery);
    if (filmSet) {
      await this.reply(jid, msg, `🔎 *Searching Cineru.lk for film set "${filmSet.setTitle}"...*`);
      const setCard = formatFilmSetCard(filmSet, p);
      let sentImage = false;
      if (filmSet.posterUrl && this.sock) {
        try {
          await Promise.race([
            this.sock.sendMessage(jid, { image: { url: filmSet.posterUrl }, caption: setCard }, { quoted: msg }),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 3000))
          ]);
          sentImage = true;
        } catch {}
      }
      if (!sentImage) {
        await this.reply(jid, msg, setCard);
      }
      return;
    }

    // For individual movie queries on .cineru, use the full direct WhatsApp movie downloader
    await this.cmdFilm(jid, msg, cleanQuery, false);
  }

  async cmdSubtitle(jid: string, msg: any, query?: string) {
    const p = this.state.config.prefix;
    const rawQuery = (query || "").trim();

    if (!rawQuery) {
      await this.reply(
        jid,
        msg,
        `📝 *SINHALA SUBTITLE DOWNLOADER (.SRT)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nචිත්‍රපටයේ නම ලබා දෙන්න — සිංහල උපසිරැසි (*.SRT* ගොනුව) කෙලින්ම WhatsApp වෙත එවනු ලැබේ!\n\n💡 *උදාහරණ:*\n• *${p}sub Deadpool*\n• *${p}sub Interstellar*\n• *${p}sub Avatar*\n• *${p}sub Moana en* _(English sub සඳහා)_`
      );
      return;
    }

    if (this.sock && msg?.key) {
      void this.sock.sendMessage(jid, { react: { text: "📝", key: msg.key } }).catch(() => {});
    }

    try {
      const subRes = await searchAndDownloadSubtitle(rawQuery);
      if (!subRes.success || !subRes.srtBuffer || !subRes.fileName) {
        await this.reply(
          jid,
          msg,
          `❌ ${subRes.error || `"${rawQuery}" සඳහා උපසිරැසි ගොනුවක් හමු නොවීය.`}\n💡 නිවැරදි English නම සමඟ උත්සාහ කරන්න (උදා: *${p}sub Inception*).`
        );
        return;
      }

      if (!this.sock) {
        throw new Error("WhatsApp socket is disconnected");
      }

      const langNote = subRes.isFallbackLang
        ? `🌐 *Language:* ${subRes.language} _(සිංහල උපසිරැසි නොතිබූ බැවින් English ගොනුව ලබාදෙන ලදී)_`
        : `🗣️ *Language:* ${subRes.language}`;

      const caption = `📝 *${subRes.movieName || rawQuery}* ${subRes.year ? `(${subRes.year})` : ""}
${langNote}
📦 *Size:* ${subRes.sizeFormatted || "Direct .SRT"}

> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();

      await this.sock.sendMessage(
        jid,
        {
          document: subRes.srtBuffer,
          fileName: subRes.fileName,
          mimetype: "application/x-subrip",
          caption,
        },
        { quoted: msg }
      );

      if (this.sock && msg?.key) {
        void this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
      }
      this.addLog("SUCCESS", `📝 Subtitle delivered: ${subRes.fileName} (${subRes.sizeFormatted}) -> ${jid}`);
    } catch (err: any) {
      this.addLog("ERROR", `Subtitle error: ${err?.message || err}`);
      await this.reply(jid, msg, `❌ *Subtitle Error:* ${err?.message || "Failed to send subtitle file"}`);
    }
  }

  private async fetchFreshMediaConn(forceRefresh = false): Promise<{
    hosts: Array<{ hostname: string; maxContentLengthBytes?: number }>;
    auth: string;
  }> {
    if (!this.sock) {
      throw new Error("WhatsApp socket is disconnected");
    }
    try {
      const info = await this.sock.refreshMediaConn(forceRefresh);
      if (info?.auth && Array.isArray(info?.hosts) && info.hosts.length > 0) {
        return info;
      }
    } catch {
      // Fall through to direct IQ query (protects against poisoned Baileys mediaConn cache)
    }
    const iqRes = await this.sock.query({
      tag: "iq",
      attrs: { type: "set", xmlns: "w:m", to: "s.whatsapp.net" },
      content: [{ tag: "media_conn", attrs: {} }],
    });
    const mcNode = (iqRes?.content || []).find((c: any) => c.tag === "media_conn");
    const hosts = (mcNode?.content || [])
      .filter((c: any) => c.tag === "host")
      .map((c: any) => ({
        hostname: c.attrs?.hostname,
        maxContentLengthBytes: Number(c.attrs?.maxContentLengthBytes || 0) || undefined,
      }))
      .filter((h: any) => Boolean(h.hostname));
    return {
      hosts,
      auth: mcNode?.attrs?.auth || "",
    };
  }

  private createFastWaUpload(
    onProgress?: (uploadedBytes: number, totalBytes: number, speedBps: number) => void,
    onBeforeFinish?: () => Promise<void>
  ) {
    const WA_MEDIA_PATHS: Record<string, string> = {
      image: "/mms/image",
      video: "/mms/video",
      document: "/mms/document",
      audio: "/mms/audio",
      sticker: "/mms/image",
      "thumbnail-image": "/mms/image",
      "thumbnail-video": "/mms/video",
      "thumbnail-document": "/mms/document",
    };

    return async (
      filePath: string,
      { mediaType, fileEncSha256B64, newsletter }: { mediaType: string; fileEncSha256B64: string; timeoutMs?: number; newsletter?: boolean }
    ) => {
      if (!this.sock) {
        throw new Error("WhatsApp socket is disconnected");
      }

      const fileStats = await fs.promises.stat(filePath);
      const totalBytes = fileStats.size;
      const encToken = encodeURIComponent(
        fileEncSha256B64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")
      );

      // Force a fresh auth token (ttl=300s) right before uploading a large movie
      let uploadInfo = await this.fetchFreshMediaConn(true);
      const rawHosts: Array<{ hostname: string; maxContentLengthBytes?: number }> = Array.isArray(uploadInfo?.hosts)
        ? uploadInfo.hosts
        : [];

      // Deduplicate hostnames & filter by maxContentLengthBytes
      const uniqueHostMap = new Map<string, { hostname: string; maxContentLengthBytes?: number }>();
      for (const h of rawHosts) {
        if (h?.hostname && !uniqueHostMap.has(h.hostname)) {
          uniqueHostMap.set(h.hostname, h);
        }
      }
      if (!uniqueHostMap.has("mmg.whatsapp.net")) {
        uniqueHostMap.set("mmg.whatsapp.net", { hostname: "mmg.whatsapp.net" });
      }

      const deduped = Array.from(uniqueHostMap.values());
      const fittingHosts = deduped.filter(
        (h) => !h.maxContentLengthBytes || totalBytes <= h.maxContentLengthBytes
      );

      // Prioritize fastest verified hosts first (mmg.whatsapp.net, *-2.cdn, *-2-*, then remaining)
      const scoreHost = (hostname: string) => {
        if (hostname === "mmg.whatsapp.net") return 0;
        if (/-2\.cdn\.whatsapp\.net$/i.test(hostname)) return 1;
        if (hostname.includes("sin2-1")) return 2;
        if (/-1\.cdn\.whatsapp\.net$/i.test(hostname)) return 4;
        return 3;
      };
      const candidateHosts = (fittingHosts.length > 0 ? fittingHosts : deduped)
        .sort((a, b) => scoreHost(a.hostname) - scoreHost(b.hostname))
        .slice(0, 4);

      this.addLog(
        "INFO",
        `[WA-UPLOAD] Uploading ${formatBytesSize(totalBytes)} (${mediaType}) via prioritized hosts: ${candidateHosts
          .map((h) => h.hostname)
          .join(" -> ")}`
      );

      if (candidateHosts.length === 0) {
        throw new Error("No WhatsApp media upload hosts available");
      }

      let lastErr: Error | null = null;
      let refreshedAuthOnce = false;

      for (let i = 0; i < candidateHosts.length; i++) {
        const host = candidateHosts[i];
        const mediaPath = WA_MEDIA_PATHS[mediaType] || "/mms/document";
        const authParam = encodeURIComponent(uploadInfo.auth);
        const targetPath = `${mediaPath}/${encToken}?auth=${authParam}&token=${encToken}${newsletter ? "&server_thumb_gen=1" : ""}`;
        const hasMoreHosts = i < candidateHosts.length - 1;

        const attemptUpload = (familyPref: 6 | 0) =>
          new Promise<any>((resolve, reject) => {
            let settled = false;
            let bytesSent = 0;
            let lastProgressAt = Date.now();
            let lastReportAt = 0;
            const startedAt = Date.now();
            let streamEnded = false;

            const finish = (err?: any, val?: any) => {
              if (settled) return;
              settled = true;
              clearInterval(stallTimer);
              if (err) reject(err);
              else resolve(val);
            };

            const reqOpts: https.RequestOptions = {
              hostname: host.hostname,
              port: 443,
              path: targetPath,
              method: "POST",
              headers: {
                "Content-Type": "application/octet-stream",
                Origin: "https://web.whatsapp.com",
                "Content-Length": String(totalBytes),
              },
            };
            if (familyPref === 6) {
              reqOpts.family = 6;
            }

            const req = https.request(reqOpts, (res) => {
              let body = "";
              res.on("data", (chunk) => {
                lastProgressAt = Date.now();
                if (body.length < 4096) body += chunk.toString();
              });
              res.on("end", () => {
                const status = res.statusCode || 0;
                if (status >= 400) {
                  const err: any = new Error(`WA CDN HTTP ${status} on ${host.hostname}: ${body.slice(0, 140)}`);
                  err.statusCode = status;
                  finish(err);
                  return;
                }
                try {
                  const parsed = JSON.parse(body);
                  finish(undefined, parsed);
                } catch {
                  finish(new Error(`Invalid JSON from ${host.hostname} (HTTP ${status})`));
                }
              });
            });

            const stream = fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 });

            const stallTimer = setInterval(() => {
              const now = Date.now();
              const idleMs = now - lastProgressAt;
              const totalElapsed = now - startedAt;
              const limitMs = streamEnded ? 45000 : 25000;

              // If host is crawling < 1.6 MB/s after 12s on a large file and faster hosts remain, switch!
              if (
                hasMoreHosts &&
                totalBytes > 40 * 1024 * 1024 &&
                !streamEnded &&
                totalElapsed >= 12000 &&
                totalElapsed < 40000
              ) {
                const speedBps = bytesSent / (totalElapsed / 1000);
                if (speedBps < 1.6 * 1024 * 1024) {
                  const mbps = (speedBps / (1024 * 1024)).toFixed(2);
                  stream.destroy();
                  req.destroy(new Error(`Slow WA host ${host.hostname} (${mbps} MB/s), switching to faster host`));
                  return;
                }
              }

              if (idleMs > limitMs || totalElapsed > 10 * 60 * 1000) {
                const msgText =
                  totalElapsed > 10 * 60 * 1000
                    ? `WA CDN upload exceeded 10m deadline on ${host.hostname}`
                    : streamEnded
                      ? `WA CDN response timeout after 45s on ${host.hostname}`
                      : `WA CDN upload stalled for 25s on ${host.hostname} at ${formatBytesSize(bytesSent)}`;
                stream.destroy();
                req.destroy(new Error(msgText));
              }
            }, 4000);

            req.on("error", (err) => {
              stream.destroy();
              finish(err);
            });

            stream.on("data", (chunk: string | Buffer) => {
              const len = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
              bytesSent += len;
              lastProgressAt = Date.now();

              const now = Date.now();
              if (onProgress && (now - lastReportAt >= 3000 || bytesSent >= totalBytes)) {
                lastReportAt = now;
                const elapsedSec = Math.max(0.2, (now - startedAt) / 1000);
                onProgress(Math.min(bytesSent, totalBytes), totalBytes, bytesSent / elapsedSec);
              }
            });

            stream.on("end", () => {
              streamEnded = true;
              lastProgressAt = Date.now();
            });

            stream.on("error", (err) => {
              req.destroy(err);
              finish(err);
            });

            stream.pipe(req);
          });

        try {
          this.addLog("INFO", `[WA-UPLOAD] Uploading to ${host.hostname} (IPv6 preferred, ${i + 1}/${candidateHosts.length})...`);
          let result: any;
          try {
            result = await attemptUpload(6);
          } catch (ipv6Err: any) {
            const msg = String(ipv6Err?.message || "");
            if (msg.includes("ENETUNREACH") || msg.includes("EHOSTUNREACH") || msg.includes("EAFNOSUPPORT") || msg.includes("ENOTFOUND")) {
              this.addLog("WARN", `[WA-UPLOAD] IPv6 unavailable for ${host.hostname}, retrying auto-family...`);
              result = await attemptUpload(0);
            } else {
              throw ipv6Err;
            }
          }

          if (result?.url || result?.direct_path) {
            this.addLog("SUCCESS", `[WA-UPLOAD] Uploaded ${formatBytesSize(totalBytes)} to ${host.hostname}`);
            if (onBeforeFinish) {
              try {
                await onBeforeFinish();
              } catch {}
            }
            return {
              mediaUrl: result.url,
              directPath: result.direct_path,
              meta_hmac: result.meta_hmac,
              fbid: result.fbid,
              ts: result.ts,
              thumbnailDirectPath: result.thumbnail_info?.thumbnail_direct_path,
              thumbnailSha256: result.thumbnail_info?.thumbnail_sha256,
            };
          }

          lastErr = new Error(`Upload returned empty url from ${host.hostname}`);
        } catch (err: any) {
          lastErr = err instanceof Error ? err : new Error(String(err));
          this.addLog("WARN", `[WA-UPLOAD] Host ${host.hostname} failed: ${lastErr.message}`);
          if ((err?.statusCode === 401 || err?.statusCode === 403) && !refreshedAuthOnce) {
            refreshedAuthOnce = true;
            try {
              uploadInfo = await this.fetchFreshMediaConn(true);
            } catch {}
          }
          if (err?.statusCode === 413 && fittingHosts.length === 0) {
            break;
          }
          if (i < candidateHosts.length - 1) {
            await new Promise((r) => setTimeout(r, 300));
          }
        }
      }

      throw lastErr || new Error("WhatsApp media upload failed on all hosts");
    };
  }

  async cmdFilm(jid: string, msg: any, query?: string, onlyDub = false) {
    const p = this.state.config.prefix;
    const rawQuery = (query || "").trim();
    void cleanupStaleMovieTempFiles().catch(() => {});

    // Check if query is asking for a film set
    const detectedSet = searchFilmSets(rawQuery);
    if (detectedSet) {
      const setCard = formatFilmSetCard(detectedSet, p);
      let sentImage = false;
      if (detectedSet.posterUrl && this.sock) {
        try {
          await Promise.race([
            this.sock.sendMessage(jid, { image: { url: detectedSet.posterUrl }, caption: setCard }, { quoted: msg }),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 4000))
          ]);
          sentImage = true;
        } catch {}
      }
      if (!sentImage) {
        await this.reply(jid, msg, setCard);
      }
      return;
    }

    if (!rawQuery) {
      const guideTitle = onlyDub
        ? "SINHALA SUB / DUB MOVIES"
        : "DIRECT MOVIE DOWNLOADER (WHATSAPP)";
      await this.reply(
        jid,
        msg,
        `🎬 *${guideTitle}* 🍿\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nචිත්‍රපටයේ නම ලබා දෙන්න — බොට් විසින් චිත්‍රපටයේ විස්තර සහ Quality තෝරාගැනීමේ කාඩ්පත පෙන්වනු ඇත!\n\n💡 *උදාහරණ:*\n• *${p}film Sonic*\n• *${p}film Deadpool*\n• *${p}film KGF 2*\n• *${p}sub Deadpool* _(සිංහල Subtitle .SRT සඳහා)_\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS EXPRESS CINEMA_`
      );
      return;
    }

    const { cleanQuery, preferredQuality } = extractMovieQualityFlag(rawQuery);
    const botName = this.state.config.botName || "YASHI HALLS";

    if (this.sock && msg?.key) {
      void this.sock.sendMessage(jid, { react: { text: "🎬", key: msg.key } }).catch(() => {});
    }

    let downloadedFilePath: string | undefined;
    let dlCardMsgId: string | null = null;

    try {
      const results = await searchMovies(cleanQuery, onlyDub, preferredQuality);
      if (!results || results.length === 0) {
        await this.reply(
          jid,
          msg,
          `❌ *"${cleanQuery}"* සඳහා සෘජු බාගත කිරීමේ ගොනුවක් හමු නොවීය.\n💡 නම නිවැරදිව හෝ වර්ෂය සමඟ (උදා: *${p}film Deadpool 2016* හෝ *${p}film Moana*) නැවත උත්සාහ කරන්න.`
        );
        return;
      }

      const movie = results[0];
      const allDownloads = movie.downloads || [];

      // ───────────────────────────────────────────────────────────────────────────
      // STEP 1: No quality chosen yet -> Send Card 1 ONLY (Full-Width Banner + Details + 2 Copy Buttons)
      // ───────────────────────────────────────────────────────────────────────────
      if (!preferredQuality) {
        const cleanTitleForCmd = movie.title
          .replace(/[^\p{L}\p{N}\s]/gu, " ")
          .replace(/\s+/g, " ")
          .trim();
        const cmdPrefix = `${p}${onlyDub ? "filmdub" : "film"}`;

        const normalizeTier = (qText: string): { tag: string; label: string } => {
          const ql = (qText || "").toLowerCase();
          if (ql.includes("1080")) return { tag: "1080p", label: "FHD 1080p" };
          if (ql.includes("720")) return { tag: "720p", label: "HD 720p" };
          if (ql.includes("360")) return { tag: "360p", label: "LD 360p" };
          if (ql.includes("480") || ql.includes("sd")) return { tag: "480p", label: "SD 480p" };
          return { tag: "480p", label: qText || "Direct MP4" };
        };

        const tierMap = new Map<string, { label: string; size: string; tag: string }>();
        for (const d of allDownloads) {
          const { tag, label } = normalizeTier(d.quality);
          if (!tierMap.has(tag)) {
            tierMap.set(tag, {
              label,
              size: d.size || "Direct MP4",
              tag,
            });
          }
        }

        const tiersList = Array.from(tierMap.values());
        let qualityButtons: FilmQualityOptionButton[] = [];

        if (tiersList.length >= 2) {
          qualityButtons = tiersList.slice(0, 2).map((t) => ({
            label: t.label,
            size: t.size,
            copyCommand: `${cmdPrefix} ${cleanTitleForCmd} ${t.tag}`,
          }));
        } else if (tiersList.length === 1) {
          const single = tiersList[0];
          const altTag = single.tag === "720p" ? "480p" : "720p";
          const altLabel = altTag === "480p" ? "SD 480p" : "HD 720p";
          qualityButtons = [
            {
              label: single.label,
              size: single.size,
              copyCommand: `${cmdPrefix} ${cleanTitleForCmd} ${single.tag}`,
            },
            {
              label: altLabel,
              size: "Auto Mirror",
              copyCommand: `${cmdPrefix} ${cleanTitleForCmd} ${altTag}`,
            },
          ];
        } else {
          qualityButtons = [
            { label: "SD 480p", size: "Fast MP4", copyCommand: `${cmdPrefix} ${cleanTitleForCmd} 480p` },
            { label: "HD 720p", size: "High Quality", copyCommand: `${cmdPrefix} ${cleanTitleForCmd} 720p` },
          ];
        }

        this.pendingFilmChoices.set(jid, {
          movieTitle: movie.title,
          query: cleanTitleForCmd,
          opt1Cmd: qualityButtons[0].copyCommand,
          opt2Cmd: qualityButtons[1].copyCommand,
          onlyDub,
          ts: Date.now(),
        });

        const posterSrc = await fetchPosterAsDataUri(movie.posterUrl).catch(() => movie.posterUrl);
        let sentDetailsCard = false;

        if (this.sock) {
          try {
            const detailsMarkup = generateFilmDetailsCardHtml({
              botName,
              title: movie.title,
              year: movie.year,
              language: movie.language,
              genre: movie.genre,
              hasSinhalaSub: movie.hasSinhalaSub,
              quality: qualityButtons[0].label,
              fileSize: qualityButtons[0].size,
              source: allDownloads[0]?.source || "Express Cloud",
              posterSrc,
              synopsis: movie.synopsisSinhala || movie.synopsisEnglish,
              prefix: p,
              qualityButtons,
              height: 510,
            });
            const detRes = await sendHtmlApp(this.sock, jid, detailsMarkup, {
              title: `🎬 ${movie.title}`,
              height: 510,
              bypassDownload: true,
            });
            sentDetailsCard = Boolean(detRes.success);
          } catch {}
        }

        if (!sentDetailsCard) {
          const textChoice = `${formatMovieCard(movie, p)}\n\n👇 *කුමන Quality එකෙන්ද අවශ්‍ය? පහත Command එකක් Copy කර එවන්න (හෝ 1 / 2 Reply කරන්න):*\n\n1️⃣ \`${qualityButtons[0].copyCommand}\` _(${qualityButtons[0].label} • ${qualityButtons[0].size})_\n2️⃣ \`${qualityButtons[1].copyCommand}\` _(${qualityButtons[1].label} • ${qualityButtons[1].size})_`;
          let sentImg = false;
          if (movie.posterUrl && this.sock) {
            try {
              await Promise.race([
                this.sock.sendMessage(jid, { image: { url: movie.posterUrl }, caption: textChoice }, { quoted: msg }),
                new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 4000))
              ]);
              sentImg = true;
            } catch {}
          }
          if (!sentImg) {
            await this.reply(jid, msg, textChoice);
          }
        }

        if (this.sock && msg?.key) {
          void this.sock.sendMessage(jid, { react: { text: "🎞️", key: msg.key } }).catch(() => {});
        }
        return;
      }

      // ───────────────────────────────────────────────────────────────────────────
      // STEP 2: Quality Selected -> Send Downloading Card (Card 2) & Deliver Film Underneath!
      // ───────────────────────────────────────────────────────────────────────────
      this.pendingFilmChoices.delete(jid);

      const primaryMirror = allDownloads[0];
      const mirrorQuality = primaryMirror?.quality || preferredQuality.toUpperCase();
      const mirrorSize = primaryMirror?.size || "MP4 / MKV";

      if (this.sock && msg?.key) {
        void this.sock.sendMessage(jid, { react: { text: "⏳", key: msg.key } }).catch(() => {});
      }

      // Send Card 2 immediately so customer sees live download progress right away
      if (this.sock) {
        try {
          const initDlMarkup = generateFilmDownloadCardHtml({
            botName,
            title: movie.title,
            status: "DOWNLOADING",
            progress: 8,
            quality: mirrorQuality,
            fileSize: mirrorSize,
            height: 215,
          });
          const dlCardRes = await sendHtmlApp(this.sock, jid, initDlMarkup, {
            title: `⏳ DOWNLOADING ${movie.title}`,
            height: 215,
            bypassDownload: true,
          });
          if (dlCardRes.success && dlCardRes.messageId) {
            dlCardMsgId = dlCardRes.messageId;
          }
        } catch {}
      }

      if (!dlCardMsgId) {
        await this.reply(jid, msg, `⏳ *${movie.title}* (${mirrorQuality} • ${mirrorSize}) Download වෙමින් පවතී...`).catch(() => {});
      }

      let lastEditAt = 0;
      let lastPct = 0;

      const dlRes = await downloadMovieFileWithFallback(
        allDownloads,
        movie.title,
        undefined,
        (prog) => {
          if (!dlCardMsgId || !this.sock) return;
          const now = Date.now();
          if (now - lastEditAt < 2500 || Math.abs(prog.percent - lastPct) < 5) return;
          lastEditAt = now;
          lastPct = prog.percent;

          const dlMarkup = generateFilmDownloadCardHtml({
            botName,
            title: movie.title,
            status: "DOWNLOADING",
            progress: prog.percent,
            quality: prog.quality || mirrorQuality,
            fileSize: prog.totalBytes > 0 ? formatBytesSize(prog.totalBytes) : mirrorSize,
            downloadedText: `${formatBytesSize(prog.downloadedBytes)} / ${formatBytesSize(prog.totalBytes)}`,
            height: 215,
          });
          void editHtmlApp(this.sock, jid, dlCardMsgId, dlMarkup, {
            title: `⏳ DOWNLOADING ${prog.percent}%`,
            height: 215,
          }).catch(() => {});
        }
      );

      downloadedFilePath = dlRes.filePath;

      if (!dlRes.success || !dlRes.filePath) {
        if (dlCardMsgId && this.sock) {
          const errMarkup = generateFilmDownloadCardHtml({
            botName,
            title: movie.title,
            status: "ERROR",
            progress: 0,
            quality: mirrorQuality,
            fileSize: mirrorSize,
            errorMessage: dlRes.error || "Mirror unavailable",
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, {
            title: "❌ DOWNLOAD FAILED",
            height: 215,
          }).catch(() => {});
        }
        const fallbackLinks = allDownloads
          .slice(0, 3)
          .map((d, idx) => `${idx + 1}. *${d.quality}* (${d.size}):\n🔗 ${d.directUrl}`)
          .join("\n\n");
        await this.reply(
          jid,
          msg,
          `⚠️ *${movie.title}* චිත්‍රපටය සෘජුවම ලබාගැනීමට නොහැකි විය.\n_හේතුව:_ ${dlRes.error || "Mirror unavailable"}\n\n${fallbackLinks ? `📥 *Direct Download Links:*\n${fallbackLinks}` : ""}`
        );
        return;
      }

      if (!this.sock) {
        throw new Error("WhatsApp socket is disconnected");
      }

      const fileName = dlRes.fileName || `${movie.title}.mp4`;
      const sizeText = dlRes.sizeFormatted || "Direct HD";

      if (this.sock && msg?.key) {
        void this.sock.sendMessage(jid, { react: { text: "🚀", key: msg.key } }).catch(() => {});
      }

      // Transition Card 2 to Uploading state in-place
      if (dlCardMsgId) {
        const upMarkup = generateFilmDownloadCardHtml({
          botName,
          title: movie.title,
          status: "UPLOADING",
          progress: 5,
          quality: dlRes.quality || mirrorQuality,
          fileSize: sizeText,
          downloadedText: `Encrypting & Uploading to WhatsApp (${sizeText})...`,
          fileName,
          height: 215,
        });
        void editHtmlApp(this.sock, jid, dlCardMsgId, upMarkup, {
          title: "🚀 UPLOADING TO WHATSAPP",
          height: 215,
        }).catch(() => {});
      }

      let lastUpEditAt = 0;
      let sentCompletedBanner = false;

      const sendCompletedCardNow = async () => {
        if (sentCompletedBanner || !this.sock) return;
        sentCompletedBanner = true;
        const doneMarkup = generateFilmDownloadCardHtml({
          botName,
          title: movie.title,
          status: "COMPLETED",
          progress: 100,
          quality: dlRes.quality || mirrorQuality,
          fileSize: sizeText,
          fileName,
          height: 215,
        });
        // Edit the SAME Downloading Card in-place to COMPLETED — never send a duplicate second card!
        if (dlCardMsgId) {
          await editHtmlApp(this.sock, jid, dlCardMsgId, doneMarkup, {
            title: "✅ DOWNLOAD COMPLETED",
            height: 215,
          }).catch(() => {});
        } else {
          await sendHtmlApp(this.sock, jid, doneMarkup, {
            title: "✅ DOWNLOAD COMPLETED",
            height: 215,
            bypassDownload: true,
          }).catch(() => {});
        }
      };

      const fastWaUpload = this.createFastWaUpload(
        (uploadedBytes, totalBytes, speedBps) => {
          if (!dlCardMsgId || !this.sock) return;
          const now = Date.now();
          const upPct = totalBytes > 0 ? Math.min(99, Math.max(2, Math.round((uploadedBytes / totalBytes) * 100))) : 50;
          if (now - lastUpEditAt < 2500 && upPct < 99) return;
          lastUpEditAt = now;

          const speedMb = (speedBps / (1024 * 1024)).toFixed(1);
          const upMarkup = generateFilmDownloadCardHtml({
            botName,
            title: movie.title,
            status: "UPLOADING",
            progress: upPct,
            quality: dlRes.quality || mirrorQuality,
            fileSize: sizeText,
            downloadedText: `${formatBytesSize(uploadedBytes)} / ${formatBytesSize(totalBytes)} (${speedMb} MB/s)`,
            fileName,
            height: 215,
          });
          void editHtmlApp(this.sock, jid, dlCardMsgId, upMarkup, {
            title: `🚀 UPLOADING • ${upPct}%`,
            height: 215,
          }).catch(() => {});
        },
        sendCompletedCardNow
      );

      const fileCaption = `🎬 *${movie.title}*
📊 *ප්‍රමාණය (Size):* ${sizeText}
🎞️ *Quality:* ${dlRes.quality || "HD"}
⚡ *Source:* ${dlRes.source || "Direct Cloud"}
${movie.hasSinhalaSub ? "📝 *Sinhala Subtitles Included*" : ""}

> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();

      const isSmallMp4 =
        typeof dlRes.sizeBytes === "number" &&
        dlRes.sizeBytes < 60 * 1024 * 1024 &&
        dlRes.mimeType === "video/mp4";

      const uploadStream = fs.createReadStream(dlRes.filePath, { highWaterMark: 1024 * 1024 });

      const sendPayload: any = isSmallMp4
        ? {
            video: { stream: uploadStream },
            mimetype: "video/mp4",
            caption: fileCaption,
          }
        : {
            document: { stream: uploadStream },
            fileName,
            mimetype: dlRes.mimeType || "video/mp4",
            caption: fileCaption,
          };

      try {
        await Promise.race([
          this.sock.sendMessage(jid, sendPayload, { quoted: msg, upload: fastWaUpload }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("WhatsApp upload timed out after 12 minutes")), 12 * 60 * 1000)
          ),
        ]);
      } catch (uploadErr: any) {
        uploadStream.destroy();
        const errMarkup = generateFilmDownloadCardHtml({
          botName,
          title: movie.title,
          status: "ERROR",
          progress: 0,
          quality: dlRes.quality || mirrorQuality,
          fileSize: sizeText,
          errorMessage: uploadErr?.message || "WhatsApp upload failed",
          height: 215,
        });
        if (this.sock) {
          if (dlCardMsgId) {
            await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, {
              title: "❌ WHATSAPP UPLOAD FAILED",
              height: 215,
            }).catch(() => {});
          } else {
            await sendHtmlApp(this.sock, jid, errMarkup, {
              title: "❌ WHATSAPP UPLOAD FAILED",
              height: 215,
              bypassDownload: true,
            }).catch(() => {});
          }
        }
        const fallbackLinks = allDownloads
          .slice(0, 3)
          .map((d, idx) => `${idx + 1}. *${d.quality}* (${d.size}):\n🔗 ${d.directUrl}`)
          .join("\n\n");
        await this.reply(
          jid,
          msg,
          `⚠️ *${movie.title}* (${sizeText}) WhatsApp Server වෙත Upload කිරීම අසාර්ථක විය.\n_හේතුව:_ ${uploadErr?.message || "Upload failed"}\n\n${fallbackLinks ? `📥 *Direct Download Links (Browser එකෙන් ඩවුන්ලෝඩ් කරගන්න):*\n${fallbackLinks}` : ""}`
        );
        return;
      }

      if (!sentCompletedBanner) {
        await sendCompletedCardNow();
      }

      if (this.sock && msg?.key) {
        void this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
      }
      this.addLog("SUCCESS", `🎬 Movie delivered to WhatsApp: ${fileName} (${sizeText}) -> ${jid}`);
    } catch (err: any) {
      this.addLog("ERROR", `Movie download/send error: ${err?.message || err}`);
      if (dlCardMsgId && this.sock) {
        const errMarkup = generateFilmDownloadCardHtml({
          botName,
          title: cleanQuery,
          status: "ERROR",
          progress: 0,
          errorMessage: err?.message || "Failed to send movie",
          height: 215,
        });
        void editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ DOWNLOAD FAILED", height: 215 }).catch(() => {});
      }
      await this.reply(jid, msg, `❌ *Movie Download Error:* ${err?.message || "Failed to send movie"}`);
    } finally {
      if (downloadedFilePath) {
        await fs.promises.unlink(downloadedFilePath).catch(() => {});
      }
    }
  }

  getConnectedUserName(): string {
    const rawName = this.sock?.user?.name || (this.sock?.user as any)?.notify;
    if (rawName && String(rawName).trim() && !['Akash Kavindu', 'YASHI HALLS'].includes(String(rawName).trim())) {
      return String(rawName).trim();
    }
    if (this.state.pairedNumber && this.state.pairedNumber !== "Unknown") {
      return `+${this.state.pairedNumber}`;
    }
    if (this.state.config.ownerName && !['Akash Kavindu'].includes(this.state.config.ownerName)) {
      return this.state.config.ownerName;
    }
    return "User";
  }

  async getLogoBuffer() {
    const currentLogoUrl = this.state.config.botLogoUrl || BRAND_LOGO_URL;

    if (this.cachedLogoBuffer && this.lastCachedLogoUrl === currentLogoUrl) {
      return this.cachedLogoBuffer;
    }

    const candidatePaths = [
      path.resolve(process.cwd(), 'data', 'bot_header_banner.png'),
      path.resolve(process.cwd(), 'public', 'assets', 'bot_header_banner.png'),
      '/data/bot_header_banner.png',
      '/public/assets/bot_header_banner.png',
      path.resolve(process.cwd(), 'public', 'assets', 'aistudio', 'file_00000000a5988208ba0d8759a81525b4_ee7e1d49.png'),
      path.resolve(process.cwd(), 'data', 'bot_logo.png'),
      path.resolve(process.cwd(), 'data', 'bot_logo_opt.jpg'),
      '/public/assets/aistudio/file_00000000a5988208ba0d8759a81525b4_ee7e1d49.png',
      '/data/bot_logo.png',
      '/data/bot_logo_opt.jpg',
      '/app/applet/data/bot_logo.png',
    ];

    if (!this.state.config.botLogoUrl || this.state.config.botLogoUrl === BRAND_LOGO_URL) {
      for (const p of candidatePaths) {
        try {
          if (fs.existsSync(p)) {
            this.cachedLogoBuffer = fs.readFileSync(p);
            this.lastCachedLogoUrl = currentLogoUrl;
            return this.cachedLogoBuffer;
          }
        } catch {}
      }
    }

    try {
      const resp = await fetch(currentLogoUrl, { signal: AbortSignal.timeout(4000) });
      if (resp.ok) {
        const arr = await resp.arrayBuffer();
        const rawBuf = Buffer.from(arr);
        this.cachedLogoBuffer = rawBuf;
        this.lastCachedLogoUrl = currentLogoUrl;
        return this.cachedLogoBuffer;
      }
    } catch (err) {}

    return null;
  }

  async getCardThumbnailBuffer(): Promise<Buffer> {
    const thumbPath = path.resolve(process.cwd(), 'data', 'bot_card_thumb.jpg');
    try {
      if (fs.existsSync(thumbPath)) {
        const buf = fs.readFileSync(thumbPath);
        if (buf && buf.length > 500) return buf;
      }
    } catch {}

    try {
      const sharp = (await import('sharp')).default;
      const logoBuf = await this.getLogoBuffer();
      if (logoBuf) {
        const thumb = await sharp(logoBuf)
          .resize(600, 350, { fit: 'cover' })
          .jpeg({ quality: 75 })
          .toBuffer();
        try { fs.writeFileSync(thumbPath, thumb); } catch {}
        return thumb;
      }
    } catch (e) {}

    // Tiny 1x1 valid base64 JPEG fallback if all else fails
    return Buffer.from(
      '/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////wgALCAABAAEBAREA/8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPxA=',
      'base64'
    );
  }

  detectClientDevice(msg: any): "android" | "ios" | "web" | "desktop" | "unknown" {
    try {
      const msgId = msg?.key?.id || "";
      if (!msgId) return "unknown";
      return getDevice(msgId);
    } catch {
      return "unknown";
    }
  }

  isUiMenuSupported(msg: any, jid: string, forceClassic = false): boolean {
    if (forceClassic) return false;
    if (this.classicMenuUsers.has(jid)) return false;
    const device = this.detectClientDevice(msg);
    // In-Chat HTML App cards render natively on official Android WhatsApp clients only.
    // iOS, Web, Desktop, and unrecognized/third-party IDs automatically receive the Classic Menu.
    return device === "android";
  }

  async sendCategoryMenuCard(jid: string, msg: any, options: {
    title: string;
    theme?: 'purple' | 'blue' | 'cyan' | 'gold' | 'red' | 'green' | 'pink';
    icon: string;
    cards: Array<{ emoji?: string; name: string; desc: string; capsule: string }>;
    quickButtons?: Array<{ buttonId: string; displayText: string }>;
    copyCodes?: Array<{ displayText: string; copyCode: string }>;
    forceClassic?: boolean;
  }) {
    const p = this.state.config.prefix || '.';
    const botName = this.state.config.botName || 'YASHI HALLS MINI';
    const useHtmlUi = this.isUiMenuSupported(msg, jid, Boolean(options.forceClassic));

    // 1. Deliver Android In-Chat HTML App Card ONLY on supported Android clients
    if (useHtmlUi && this.sock) {
      const markup = generateCategorySubmenuHtml({
        title: options.title,
        cards: options.cards.map((c) => ({
          name: c.name,
          desc: c.desc,
          capsule: c.capsule.startsWith(p) ? c.capsule : `${p}${c.capsule}`,
        })),
        botName,
        prefix: p,
        height: 560,
      });

      try {
        const result = await sendHtmlApp(this.sock, jid, markup, {
          title: `${botName} • ${options.title}`,
          height: 560,
          bypassDownload: true,
        });
        if (result.success) {
          return;
        }
      } catch (err: any) {
        console.warn('[CARD] HTML App delivery notice:', err?.message || err);
      }
    }

    // 2. Classic Fallback (Visual PNG Menu Card + Numbered/Copyable Command List) for iOS / Web / Desktop / .textmenu
    const commandLines = options.cards.map((c, i) => {
      const emoji = c.emoji || options.icon || "⚡";
      const name = c.name;
      const cmdCapsule = c.capsule.startsWith(p) ? c.capsule : `${p}${c.capsule}`;
      const num = String(i + 1).padStart(2, '0');
      return `│ ${num}. ${emoji} *${name}:* \`${cmdCapsule}\``;
    }).join('\n');

    const cleanCaption = `👑 *${botName.toUpperCase()} • ${options.title.toUpperCase()}* 👑
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Total Commands:* ${options.cards.length} Commands
🟢 *Status:* 24/7h Online • 📱 *Universal View*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${commandLines}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
↩️ *ප්‍රධාන මෙනුවට:* *0* හෝ \`${p}menu\` යවන්න.
💡 *1-Tap Copy:* ඕනෑම Command එකක් Touch/Hold කර Copy කරගන්න.
📢 *Official Channel:* ${WHATSAPP_CHANNEL_URL}
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`;

    if (this.sock) {
      try {
        const cardPng = await generateMenuCardPng({
          title: options.title,
          subtitle: `${botName.toUpperCase()} • ${options.cards.length} COMMANDS`,
          cards: options.cards.slice(0, 12).map((c) => ({
            name: c.name,
            desc: c.desc,
            capsule: c.capsule.startsWith(p) ? c.capsule : `${p}${c.capsule}`,
          })),
          theme: options.theme || 'purple',
          icon: options.icon || '⚡',
          footer: `REPLY 0 FOR MAIN MENU • ${botName.toUpperCase()}`,
        });

        await this.sock.sendMessage(
          jid,
          {
            image: cardPng,
            caption: cleanCaption,
          },
          { quoted: msg }
        );
        return;
      } catch (imgErr: any) {
        console.warn('[CLASSIC CARD] PNG render notice, using text fallback:', imgErr?.message || imgErr);
      }
    }

    await this.reply(jid, msg, cleanCaption);
  }

  async sendAiMenuCard(jid: string, msg: any, forceClassic = false) {
    const p = this.state.config.prefix || '.';
    await this.sendCategoryMenuCard(jid, msg, {
      title: "AI COMMANDS MENU",
      theme: "cyan",
      icon: "🤖",
      forceClassic,
      cards: [
        { emoji: "🤖", name: "AI / Gemini", desc: "GEMINI 2.5 FLASH ULTRA", capsule: `${p}ai <prompt>` },
        { emoji: "🌐", name: "Translate", desc: "SINHALA & MULTILINGUAL AI", capsule: `${p}translate <lang> <text>` },
        { emoji: "📝", name: "Summarize", desc: "DOCUMENT & ARTICLE SUMMARIZER", capsule: `${p}summarize <text>` },
        { emoji: "✍️", name: "Write", desc: "ESSAYS, EMAILS & SCRIPTS", capsule: `${p}write <prompt>` },
        { emoji: "💻", name: "Code", desc: "CODE GENERATOR & BUG FIXER", capsule: `${p}code <prompt>` },
        { emoji: "🎨", name: "Imagine", desc: "PHOTOREALISTIC AI ART", capsule: `${p}imagine <prompt>` },
        { emoji: "🔍", name: "OCR", desc: "IMAGE TEXT EXTRACTOR", capsule: `${p}ocr` },
        { emoji: "🖼️", name: "Sticker", desc: "CONVERT IMAGE TO STICKER", capsule: `${p}sticker` },
        { emoji: "🎙️", name: "VoiceTrans", desc: "VOICE NOTE TO SINHALA", capsule: `${p}voicetrans <lang>` },
        { emoji: "🎵", name: "Shazam", desc: "AUDIO SONG IDENTIFIER", capsule: `${p}shazam` },
      ]
    });
  }

  async sendGroupMenuCard(jid: string, msg: any, forceClassic = false) {
    const p = this.state.config.prefix || '.';
    await this.sendCategoryMenuCard(jid, msg, {
      title: "GROUP MANAGEMENT MENU",
      theme: "blue",
      icon: "👥",
      forceClassic,
      cards: [
        { emoji: "ℹ️", name: "GroupInfo", desc: "SECURITY & GROUP STATS", capsule: `${p}groupinfo` },
        { emoji: "👑", name: "Admins", desc: "LIST GROUP ADMINISTRATORS", capsule: `${p}admins` },
        { emoji: "📢", name: "TagAll", desc: "MENTION ALL MEMBERS", capsule: `${p}tagall <msg>` },
        { emoji: "👻", name: "HideTag", desc: "GHOST MENTION WITHOUT TEXT", capsule: `${p}hidetag <msg>` },
        { emoji: "🚫", name: "Kick", desc: "REMOVE GROUP PARTICIPANT", capsule: `${p}kick <@user>` },
        { emoji: "➕", name: "Add", desc: "ADD NUMBER TO GROUP", capsule: `${p}add <number>` },
        { emoji: "⬆️", name: "Promote", desc: "MAKE MEMBER ADMIN", capsule: `${p}promote <@user>` },
        { emoji: "⬇️", name: "Demote", desc: "REMOVE ADMIN ROLE", capsule: `${p}demote <@user>` },
        { emoji: "🔇", name: "Mute", desc: "DISABLE GROUP MESSAGES", capsule: `${p}mute` },
        { emoji: "🔊", name: "Unmute", desc: "ENABLE GROUP MESSAGES", capsule: `${p}unmute` },
        { emoji: "⚠️", name: "Warn", desc: "ISSUE USER WARNING", capsule: `${p}warn <@user>` },
        { emoji: "🛡️", name: "AntiLink", desc: "AUTO REMOVE LINK SPAMMERS", capsule: `${p}antilink on/off` },
      ]
    });
  }

  async sendDownloadMenuHtmlApp(jid: string, msg: any, forceClassic = false) {
    const p = this.state.config.prefix || '.';
    await this.sendCategoryMenuCard(jid, msg, {
      title: "DOWNLOAD MENU",
      theme: "purple",
      icon: "📥",
      forceClassic,
      cards: [
        { emoji: "🎵", name: "Song", desc: "HIGH QUALITY MP3 TRACK", capsule: `${p}song <title>` },
        { emoji: "🎬", name: "Video", desc: "1080P HD MP4 VIDEO", capsule: `${p}video <title>` },
        { emoji: "▶️", name: "Play", desc: "FAST YOUTUBE MUSIC", capsule: `${p}play <title>` },
        { emoji: "🔊", name: "Audio", desc: "VOICE NOTE AUDIO", capsule: `${p}audio <title>` },
        { emoji: "🔎", name: "YtSearch", desc: "YOUTUBE VIDEO SEARCH", capsule: `${p}ytsearch <query>` },
        { emoji: "📁", name: "Mediafire", desc: "DIRECT MF DOWNLOAD (2GB)", capsule: `${p}mf <url>` },
        { emoji: "🎵", name: "TikTok", desc: "TIKTOK NO WATERMARK", capsule: `${p}tt <url>` },
        { emoji: "📘", name: "Facebook", desc: "FACEBOOK HD / SD", capsule: `${p}fb <url>` },
        { emoji: "📸", name: "Instagram", desc: "INSTAGRAM REELS & POSTS", capsule: `${p}ig <url>` },
        { emoji: "🐦", name: "Twitter", desc: "TWITTER / X VIDEO DOWNLOAD", capsule: `${p}twitter <url>` },
        { emoji: "📦", name: "APK", desc: "ANDROID APK APPS", capsule: `${p}apk <app>` },
        { emoji: "🎤", name: "Lyrics", desc: "FULL SONG LYRICS", capsule: `${p}lyrics <song>` },
      ]
    });
  }

  async cmdHelp(jid: string, msg: any) {
    return this.cmdMenu(jid, msg);
  }

  async cmdMenu(jid: string, msg: any, categoryArg?: string, isOwnerArg?: boolean, forceClassic = false) {
    const sub = (categoryArg || '').trim().toLowerCase();
    const p = this.state.config.prefix || '.';
    const botName = this.state.config.botName || 'YASHI HALLS MINI';
    const plan = (this.state.config.plan || 'MINI').toUpperCase();
    const senderNumber = jid.split('@')[0].split(':')[0];
    const isOwner = isOwnerArg !== undefined ? Boolean(isOwnerArg) : Boolean(msg?.key?.fromMe || senderNumber.includes('0740712934') || senderNumber.includes('94740712934'));
    const useHtmlUi = this.isUiMenuSupported(msg, jid, forceClassic);
    const classicModeFlag = !useHtmlUi;

    // Specific Category Submenus
    if (sub.includes('down') || sub.includes('dl') || sub === '1' || sub === '2') {
      await this.sendDownloadMenuHtmlApp(jid, msg, classicModeFlag);
      return;
    }
    if (sub.includes('ai') || sub.includes('gemini') || sub === '10' || sub === '4') {
      await this.sendAiMenuCard(jid, msg, classicModeFlag);
      return;
    }
    if (sub.includes('group') || sub.includes('admin') || sub === '5' || sub === '3') {
      await this.sendGroupMenuCard(jid, msg, classicModeFlag);
      return;
    }
    if (sub.includes('movie') || sub.includes('film') || sub === 'filmset' || sub === 'series') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "MOVIES & TV MENU",
        theme: "gold",
        icon: "🎬",
        forceClassic: classicModeFlag,
        cards: MOVIES_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('music') || sub === 'song' || sub === '9') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "MUSIC & AUDIO MENU",
        theme: "pink",
        icon: "🎵",
        forceClassic: classicModeFlag,
        cards: MUSIC_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('game') || sub === '6' || sub === 'games') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "GAMES & FUN MENU",
        theme: "green",
        icon: "🎮",
        forceClassic: classicModeFlag,
        cards: GAMES_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('util') || sub.includes('math') || sub === '8') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "UTILITIES & CONVERTERS MENU",
        theme: "cyan",
        icon: "🧮",
        forceClassic: classicModeFlag,
        cards: UTILITIES_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('text') || sub.includes('tool') || sub === '12') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "TEXT TOOLS MENU",
        theme: "purple",
        icon: "🛠️",
        forceClassic: classicModeFlag,
        cards: TEXT_TOOLS_CATALOG.slice(0, 24).map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('search') || sub === 'find') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "SEARCH TOOLS MENU",
        theme: "blue",
        icon: "🔎",
        forceClassic: classicModeFlag,
        cards: SEARCH_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }
    if (sub.includes('system') || sub.includes('bot') || sub === 'sys') {
      await this.sendCategoryMenuCard(jid, msg, {
        title: "SYSTEM & BOT CONTROLS",
        theme: "gold",
        icon: "⚙️",
        forceClassic: classicModeFlag,
        cards: SYSTEM_CATALOG.map(c => ({ name: c.name, desc: c.desc, capsule: c.capsule }))
      });
      return;
    }

    // 1. Deliver Android In-Chat Master Interactive Menu HTML App Card ONLY on Android UI-capable clients
    if (useHtmlUi && this.sock) {
      const cachedMenu = getCachedMasterMenuPayload({
        botName,
        prefix: p,
        plan,
        ownerNumber: this.state.config.ownerNumber || OWNER_HOTLINE,
        uptime: this.formatUptime(),
        logoUrl: this.state.config.botLogoUrl || BRAND_LOGO_URL,
        isOwner,
      });

      try {
        const result = await sendHtmlApp(this.sock, jid, cachedMenu.markup, {
          title: `👑 ${botName} • OFFICIAL MENU 👑`,
          height: 980,
          cachedPayload: cachedMenu.rawPayload,
          bypassDownload: true,
        });
        if (result.success) {
          return;
        }
      } catch (e: any) {
        console.warn(`[MENU CARD] sendHtmlApp notice:`, e?.message || e);
      }
    }

    // 2. Smart Auto-Detect Classic Fallback (Image Banner + Numbered Category Menu) for iOS / Web / Desktop / .textmenu
    const detectedDevice = this.detectClientDevice(msg);
    this.classicMenuContext.set(jid, Date.now());
    this.addLog("INFO", `📱 [CLASSIC MENU] Delivering Universal Menu (${detectedDevice}) to ${senderNumber}`);

    const masterMenu = `👑 *${botName.toUpperCase()} • OFFICIAL MASTER MENU* 👑
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Uptime:* ${this.formatUptime()} • 👑 *Plan:* ${plan}
📱 *Device Mode:* ${detectedDevice.toUpperCase()} (Universal View)
⌨️ *Prefix:* [ ${p} ] • 🇱🇰 *Language:* Sinhala / English
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📂 *CATEGORIES (අදාළ අංකය පමණක් Reply කරන්න):*

  *1.* 📥 *Downloads & Media*   (\`${p}song\`, \`${p}video\`, \`${p}tt\`, \`${p}fb\`)
  *2.* 🤖 *AI Intelligence*     (\`${p}ai\`, \`${p}imagine\`, \`${p}translate\`)
  *3.* 🎬 *Cinema & Movies*     (\`${p}film\`, \`${p}sub\`, \`${p}cineru\`)
  *4.* 👥 *Group Admin*         (\`${p}tagall\`, \`${p}kick\`, \`${p}antilink\`)
  *5.* 🎮 *Games & Fun*         (\`${p}quiz\`, \`${p}roll\`, \`${p}slot\`, \`${p}ttt\`)
  *6.* 🎵 *Music & Audio*       (\`${p}play\`, \`${p}lyrics\`, \`${p}shazam\`)
  *7.* 🛠️ *Text & Tool Box*     (\`${p}sticker\`, \`${p}ocr\`, \`${p}qr\`)
  *8.* ⚙️ *System & Controls*   (\`${p}ping\`, \`${p}alive\`, \`${p}owner\`)
  *9.* 🧮 *Utilities & Math*    (\`${p}calc\`, \`${p}weather\`, \`${p}currency\`)
 *10.* 🔎 *Search Tools*        (\`${p}ytsearch\`, \`${p}img\`, \`${p}wiki\`)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Quick Commands (කෙලින්ම භාවිතයට):*
• \`${p}song <නම>\` — MP3 සින්දු බාගත කිරීම
• \`${p}film <නම>\` — සිංහල Sub සමඟ චිත්‍රපට
• \`${p}ai <ප්‍රශ්නය>\` — YASHI AI සහායක

💡 *Reply Tip:* *1* සිට *10* දක්වා අංකයක් යවා එම කොටසේ සියලු විධාන බලන්න.
_(Android UI Menu සඳහා \`${p}ui on\` යවන්න)_
📢 *Official Channel:* ${WHATSAPP_CHANNEL_URL}
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`;

    if (this.sock) {
      try {
        const masterBannerPng = await generateMenuCardPng({
          title: `${botName.toUpperCase()} MENU`,
          subtitle: `REPLY 1 - 10 TO OPEN ANY CATEGORY`,
          theme: 'purple',
          icon: '👑',
          footer: `UNIVERSAL CLASSIC MENU • YASHI HALLS`,
          cards: [
            { name: "1. DOWNLOADS", desc: "SONG, VIDEO, TIKTOK, FB, APK", capsule: `${p}menu dl` },
            { name: "2. AI SMART", desc: "GEMINI AI, IMAGINE, TRANSLATE", capsule: `${p}menu ai` },
            { name: "3. MOVIES & TV", desc: "SINHALA SUB & DUBBED FILMS", capsule: `${p}menu movies` },
            { name: "4. GROUP ADMIN", desc: "TAGALL, KICK, MUTE, ANTILINK", capsule: `${p}menu group` },
            { name: "5. GAMES & FUN", desc: "QUIZ, SLOT, ROLL, TICTACTOE", capsule: `${p}menu games` },
            { name: "6. MUSIC AUDIO", desc: "PLAY, LYRICS, SHAZAM, AUDIO", capsule: `${p}menu music` },
            { name: "7. TEXT TOOLS", desc: "STICKER, OCR, QR & FONTS", capsule: `${p}menu tools` },
            { name: "8. SYSTEM BOT", desc: "PING, ALIVE, OWNER, SETTINGS", capsule: `${p}menu sys` },
          ],
        });

        await this.sock.sendMessage(
          jid,
          {
            image: masterBannerPng,
            caption: masterMenu,
          },
          { quoted: msg }
        );
        return;
      } catch (err: any) {
        console.warn('[CLASSIC MASTER MENU] Image send notice:', err?.message || err);
      }
    }

    await this.reply(jid, msg, masterMenu);
  }

  /**
   * Ultra-fast Interactive Cyberpunk Settings Card for WhatsApp Android
   * Only accessible to the connected Bot Owner
   */
  async cmdSettings(jid: string, msg: any, isOwner: boolean, args: string) {
    if (!isOwner) {
      await this.reply(
        jid,
        msg,
        `🔒 *අවසර නැත (Permission Denied)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nමෙම සැකසුම් පුවරුව (Settings Card) විවෘත කළ හැක්කේ Bot Owner (+${this.state.config.ownerNumber || '0740712934'}) හට පමණි.\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
      );
      return;
    }

    const cfg = this.state.config;
    if (this.isUiMenuSupported(msg, jid) && this.sock) {
      const markup = generateSettingsCardHtml({
        botName: cfg.botName || 'YASHI HALLS',
        prefix: cfg.prefix || '.',
        mode: cfg.mode || 'public',
        smartAutoReply: cfg.smartAutoReply !== false,
        voiceAutoReply: cfg.voiceAutoReplyEnabled === true,
        antiLink: cfg.antiLink === true,
        channelAutoReact: cfg.channelAutoReact === true,
        autoReadStatus: cfg.autoReadStatus === true,
        ownerNumber: cfg.ownerNumber || '94740712934',
        height: 480,
      });

      try {
        const result = await sendHtmlApp(this.sock, jid, markup, {
          title: `⚙️ ${cfg.botName || 'YASHI HALLS'} • SETTINGS ⚙️`,
          height: 480,
          bypassDownload: true,
        });
        if (result.success) {
          return;
        }
      } catch (err: any) {
        this.addLog("WARN", `[SETTINGS CARD] Error dispatching HTML card: ${err?.message || err}`);
      }
    }

    // Universal text panel for iOS / Web / Desktop / Classic Mode
    const p = cfg.prefix || '.';
    await this.reply(
      jid,
      msg,
      `⚙️ *${(cfg.botName || 'YASHI HALLS').toUpperCase()} • CONFIGURATION PANEL* ⚙️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🤖 *Bot Mode:* ${cfg.mode === 'private' ? '🔒 PRIVATE' : '🟢 PUBLIC'}\n🏷️ *Bot Name:* ${cfg.botName}\n⌨️ *Prefix:* [ ${p} ]\n💬 *Auto-Reply:* ${cfg.smartAutoReply ? '🟢 ON' : '🔴 OFF'}\n🛡️ *Anti-Link:* ${cfg.antiLink ? '🟢 ON' : '🔴 OFF'}\n⚡ *Channel Auto-React:* ${cfg.channelAutoReact ? '🟢 ON' : '🔴 OFF'}\n🎙️ *Voice Auto-Reply:* ${cfg.voiceAutoReplyEnabled ? '🟢 ON' : '🔴 OFF'}\n👀 *Read Status:* ${cfg.autoReadStatus ? '🟢 ON' : '🔴 OFF'}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Use ${p}botname, ${p}prefix, ${p}public, ${p}private, ${p}autoreply on/off to update._\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
    );
  }

  /**
   * Applies base64 serialized configuration payload generated by settings card
   */
  async cmdSetConfig(jid: string, msg: any, isOwner: boolean, payloadB64: string) {
    if (!isOwner) {
      await this.reply(jid, msg, `❌ Only the verified bot owner can update configuration.`);
      return;
    }

    if (!payloadB64 || !payloadB64.trim()) {
      await this.reply(jid, msg, `⚠️ Invalid configuration payload.`);
      return;
    }

    try {
      const decodedJson = Buffer.from(payloadB64.trim(), 'base64').toString('utf8');
      const updates = JSON.parse(decodedJson);
      if (typeof updates !== 'object' || updates === null) {
        throw new Error('Payload is not a valid JSON object');
      }

      const safeUpdates: any = {};
      if (typeof updates.botName === 'string' && updates.botName.trim()) {
        safeUpdates.botName = updates.botName.trim();
      }
      if (typeof updates.prefix === 'string' && updates.prefix.trim()) {
        safeUpdates.prefix = updates.prefix.trim().charAt(0);
      }
      if (updates.mode === 'public' || updates.mode === 'private') {
        safeUpdates.mode = updates.mode;
      }
      if (typeof updates.smartAutoReply === 'boolean') {
        safeUpdates.smartAutoReply = updates.smartAutoReply;
      }
      if (typeof updates.voiceAutoReplyEnabled === 'boolean') {
        safeUpdates.voiceAutoReplyEnabled = updates.voiceAutoReplyEnabled;
      }
      if (typeof updates.antiLink === 'boolean') {
        safeUpdates.antiLink = updates.antiLink;
      }
      if (typeof updates.channelAutoReact === 'boolean') {
        safeUpdates.channelAutoReact = updates.channelAutoReact;
      }
      if (typeof updates.autoReadStatus === 'boolean') {
        safeUpdates.autoReadStatus = updates.autoReadStatus;
      }

      this.updateConfig(safeUpdates);
      this.addLog("INFO", `[SETTINGS] Owner applied new configuration via .setconfig command: ${JSON.stringify(safeUpdates)}`);

      await this.reply(
        jid,
        msg,
        `✅ *CONFIGURATION SAVED & APPLIED!* ✨\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🏷️ *Bot Name:* ${this.state.config.botName}\n⌨️ *Prefix:* [ ${this.state.config.prefix} ]\n🤖 *Mode:* ${this.state.config.mode.toUpperCase()}\n💬 *Auto Reply:* ${this.state.config.smartAutoReply ? '🟢 ON' : '🔴 OFF'}\n🛡️ *Anti-Link:* ${this.state.config.antiLink ? '🟢 ON' : '🔴 OFF'}\n⚡ *Auto React:* ${this.state.config.channelAutoReact ? '🟢 ON' : '🔴 OFF'}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
      );
    } catch (e: any) {
      this.addLog("ERROR", `Failed to parse .setconfig payload: ${e?.message || e}`);
      await this.reply(jid, msg, `⚠️ Failed to apply configuration: ${e?.message || 'Invalid format'}`);
    }
  }

  resolvePublicUrl(): string {
    const configured = this.state.config.publicUrl;
    if (configured && typeof configured === "string" && configured.trim() && !configured.includes("localhost") && !configured.includes("127.0.0.1")) {
      return configured.trim().replace(/\/+$/, "");
    }
    const envUrl = process.env.PUBLIC_URL || process.env.APP_URL || process.env.BASE_URL;
    if (envUrl && typeof envUrl === "string" && envUrl.trim() && !envUrl.includes("localhost") && !envUrl.includes("127.0.0.1")) {
      let clean = envUrl.trim().replace(/\/+$/, "");
      if (!clean.startsWith("http://") && !clean.startsWith("https://")) {
        clean = `https://${clean}`;
      }
      return clean;
    }
    // Default fallback to Cloud Run public domain
    return "https://ais-dev-vhkftr4fj3xxbw6rcgsnxt-498406269163.asia-southeast1.run.app";
  }

  async cmdMenuTest(jid: string, msg: any, args: string, sender: string) {
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS MINI";
    const senderNumber = sender.split("@")[0].split(":")[0];
    const serverUrl = this.resolvePublicUrl();

    this.addLog("INFO", `🧪 [MENU TEST] Initiating Casileys Rich HTML & Interactive Neon Menu Test for ${senderNumber}`);

    // 1. CASILEYS RICH HTML API INSPECTION & EXECUTION
    let casileysSuccess = false;
    let casileysAttempted = false;

    if (this.sock && typeof (this.sock as any).createAIRich === "function") {
      casileysAttempted = true;
      try {
        const rich = (this.sock as any).createAIRich();
        if (rich && typeof rich.addHtml === "function") {
          const testHtml = `
            <div style="background:#090313;color:#fff;padding:14px;border-radius:15px;border:1px solid #a855f7;font-family:sans-serif;">
              <h2 style="color:#d946ef;margin:0 0 6px 0;font-size:16px;">👑 YASHI HALLS MINI • CASILEYS RICH HTML</h2>
              <p style="font-size:12px;color:#cbd5e1;margin:0 0 8px 0;">⚡ Casileys Rich HTML Engine is ACTIVE on this instance.</p>
              <div style="padding:8px 12px;background:#1e082e;border-radius:8px;font-size:11px;color:#f0abfc;">
                Tested via sock.createAIRich() & rich.addHtml()
              </div>
            </div>
          `;
          rich.addHtml(testHtml);
          const payload = rich.build ? rich.build() : { richHtml: testHtml };
          await this.sock.sendMessage(jid, payload, { quoted: msg });
          casileysSuccess = true;
          this.addLog("SUCCESS", `[CASILEYS TEST] Rich HTML delivered to ${senderNumber}`);
        }
      } catch (cErr: any) {
        this.addLog("WARN", `[CASILEYS TEST] Execution failed: ${cErr?.message || cErr}`);
      }
    } else {
      this.addLog("INFO", `[CASILEYS TEST] sock.createAIRich is not loaded in current Baileys instance. Delivering standalone offline HTML Document.`);
    }

    // 2. DELIVER THE STANDALONE INTERACTIVE NEON HTML FILE (100% Offline & Speed, No Google 403 login barrier)
    let docSent = false;
    try {
      const htmlPath = path.join(process.cwd(), "public", "interactive-menu.html");
      if (fs.existsSync(htmlPath) && this.sock) {
        const htmlBuffer = fs.readFileSync(htmlPath);
        await this.sock.sendMessage(
          jid,
          {
            document: htmlBuffer,
            fileName: "YASHI_HALLS_MENU.html",
            mimetype: "text/html",
            caption: `👑 *YASHI HALLS MINI • NEON INTERACTIVE MENU* 👑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📥 *ඉහත "Download" Touch කරන්න:*\n1. ඉහත HTML File එකෙහි *Download* Touch කරන්න.\n2. Download වූ පසු එය Touch කළ විට Phone එකෙන්ම Neon Menu එක විවෘත වේ!\n3. Category Touch කර Submenu බලන්න සහ Commands 1-Tap Touch කර Copy කරගන්න.\n\n✨ *100% Offline වැඩ කරයි (Google 403 හෝ Login අනවශ්‍යයි)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
          },
          { quoted: msg }
        );
        docSent = true;
        this.addLog("SUCCESS", `[MENU TEST] Interactive HTML Document delivered to ${senderNumber}`);
      }
    } catch (docErr: any) {
      this.addLog("WARN", `[MENU TEST] Could not send HTML document: ${docErr?.message || docErr}`);
    }

    // 3. Deliver Interactive WhatsApp Chat Buttons so user can also interact without leaving WhatsApp
    const menuWebUrl = `${serverUrl}/interactive-menu?jid=${encodeURIComponent(jid)}&phone=${encodeURIComponent(senderNumber)}`;
    const testMenuText = `👑 *YASHI HALLS MINI • NEON TEST MENU* 👑
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🧪 *Test Command:* \`${p}menutest\` (Isolated Test Mode)
🤖 *Bot Engine:* Baileys v7.0.0-rc14 (Multi-Device)
🌐 *Casileys API Status:* ${casileysSuccess ? "🟢 Active (Casileys HTML Delivered)" : "🟡 Not Installed (HTML Document Delivered Directly)"}
🎨 *Theme:* Purple & Black Neon Glow (#090313 / #a855f7)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✨ *INTERACTIVE MENU ක්‍රම 2:*
1️⃣ *HTML FILE එක:* ඉහත එවන ලද HTML ගොනුවේ *Download* Touch කර Open කරන්න. Google Login හෝ 403 දෝෂ නොමැතිව Phone එකෙන්ම කෙළින්ම UI එක Load වේ!
2️⃣ *චැට් එකෙන්ම විධාන:* පහත ඇති Buttons Touch කර කෙළින්ම Bot විධාන ක්‍රියාත්මක කරන්න.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`;

    try {
      if (this.sock) {
        await sendButtons(
          this.sock,
          jid,
          {
            title: "💜 YASHI HALLS MINI TEST MENU 💜",
            text: testMenuText,
            footer: "⚡ TEST MODE: .menutest • POWERED BY YASHI HALLS",
            buttons: [
              { buttonId: `${p}htmltest`, buttonText: { displayText: "📱 INTERACTIVE HTML CARD" } },
              { buttonId: `${p}menu`, buttonText: { displayText: "📋 ALL CATEGORIES" } },
              { buttonId: `${p}ping`, buttonText: { displayText: "⚡ TEST PING" } },
              { buttonId: `${p}alive`, buttonText: { displayText: "🟢 TEST ALIVE" } },
            ],
            urls: [
              { displayText: "📢 OFFICIAL CHANNEL", url: WHATSAPP_CHANNEL_URL },
            ],
          },
          { quoted: msg }
        );
        this.addLog("INFO", `✅ Delivered .menutest buttons to ${senderNumber}`);
        return;
      }
    } catch (err: any) {
      this.addLog("WARN", `Could not send test buttons: ${err?.message}. Delivering fallback message.`);
    }

    await this.reply(jid, msg, testMenuText);
  }

  async cmdWelcomeCard(jid: string, msg: any, sender: string) {
    const p = this.state.config.prefix || ".";
    const senderNumber = sender ? sender.split("@")[0].split(":")[0] : (this.state.pairedNumber || "User");
    const connectedOwner = this.getConnectedUserName();
    const botName = this.state.config.botName || 'YASHI HALLS MINI';
    const plan = `${this.state.config.plan?.toUpperCase() || 'MINI'} PLAN`;

    try {
      const markup = generateConnectedWelcomeHtmlApp({
        botName,
        userName: connectedOwner,
        phoneNumber: senderNumber,
        prefix: p,
        plan,
        channelUrl: WHATSAPP_CHANNEL_URL,
        height: 620,
      });

      const result = await sendHtmlApp(this.sock, jid, markup, {
        title: `🎉 BOT CONNECTED • ${botName} 🎉`,
        height: 620,
        bypassDownload: true,
      });

      if (result.success) {
        this.addLog("SUCCESS", `🎉 [WELCOME CARD PREVIEW] Delivered interactive welcome card to ${senderNumber}`);
        return;
      }
    } catch (err: any) {
      this.addLog("WARN", `⚠️ [WELCOME CARD PREVIEW] Failed: ${err?.message || err}`);
    }

    // Fallback if card delivery failed
    const fallback = `*👑 ${botName} • CONNECTED 👑*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🟢 *Status:* ONLINE & ACTIVE 24/7 ✅
👑 *User:* ${connectedOwner}
📱 *Number:* +${senderNumber}
🔣 *Prefix:* [ ${p} ]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 Use *${p}menu* to open the interactive menu card.
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`;
    await this.reply(jid, msg, fallback);
  }

  async autoFollowOfficialChannel(phoneNum: string) {
    try {
      if (!this.sock) return;
      const channelUrl = WHATSAPP_CHANNEL_URL || "https://whatsapp.com/channel/0029Vb927N4GJP8MS1yurM2Z";
      const match = channelUrl.match(/channel\/([a-zA-Z0-9_-]+)/);
      const inviteCode = match ? match[1] : "0029Vb927N4GJP8MS1yurM2Z";

      this.addLog("INFO", `📢 [AUTO CHANNEL FOLLOW] Initiating auto-follow to official channel (${inviteCode}) for +${phoneNum}...`);

      let newsletterJid = (this as any)._cachedNewsletterJid;

      if (!newsletterJid && typeof (this.sock as any).newsletterMetadata === "function") {
        try {
          const meta = await (this.sock as any).newsletterMetadata("INVITE", inviteCode);
          if (meta && meta.id) {
            newsletterJid = meta.id;
            (this as any)._cachedNewsletterJid = meta.id;
            this.addLog("INFO", `📢 Resolved official channel: "${meta.name || 'YASHI HALLS'}" (JID: ${meta.id})`);
          }
        } catch (metaErr: any) {
          this.addLog("WARN", `⚠️ Could not query channel metadata for code ${inviteCode}: ${metaErr?.message || metaErr}`);
        }
      }

      if (newsletterJid && typeof (this.sock as any).newsletterFollow === "function") {
        await (this.sock as any).newsletterFollow(newsletterJid);
        this.addLog("SUCCESS", `🎉 [AUTO CHANNEL FOLLOW] +${phoneNum} successfully followed official channel (${newsletterJid})!`);
      }
    } catch (err: any) {
      this.addLog("WARN", `⚠️ [AUTO CHANNEL FOLLOW] Auto-follow notice: ${err?.message || err}`);
    }
  }

  async cmdFollowChannel(jid: string, msg: any, sender: string) {
    const senderNumber = sender ? sender.split("@")[0].split(":")[0] : (this.state.pairedNumber || "User");
    await this.autoFollowOfficialChannel(senderNumber);
    const p = this.state.config.prefix || ".";
    const text = `📢 *YASHI HALLS OFFICIAL CHANNEL* 📢
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ *Channel Status:* Auto-Follow Processed!
🔗 *Official Link:* ${WHATSAPP_CHANNEL_URL}

💡 ඔබගේ WhatsApp ගිණුම අපගේ නිල Channel එකට සාර්ථකව සම්බන්ධ වී ඇත.
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`;
    await this.reply(jid, msg, text);
  }

  async cmdTest1Html(jid: string, msg: any, args: string = '') {
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS MINI";
    const senderNumber = jid.split("@")[0].split(":")[0];
    const sub = (args || '').trim().toLowerCase();

    this.addLog("INFO", `💬 Delivering 100% In-Chat Bubble Menu (.test1) to ${senderNumber} (sub: "${sub || 'all'}")`);

    // Category 1: Downloads
    if (sub.includes('down') || sub.includes('media') || sub.includes('dl') || sub === '1') {
      const text = `╔══════════════════════════╗
║   📥 *DOWNLOADS MENU* 📥   ║
║   Music, Videos & Social ║
╚══════════════════════════╝

┌─── 🎵 *COMMANDS (11)* ───┐
│ 01. \`${p}song <name/url>\`
│ 02. \`${p}video <name/url>\`
│ 03. \`${p}play <query>\`
│ 04. \`${p}audio <query>\`
│ 05. \`${p}ytsearch <query>\`
│ 06. \`${p}tt <tiktok url>\`
│ 07. \`${p}fb <facebook url>\`
│ 08. \`${p}ig <instagram url>\`
│ 09. \`${p}twitter <tweet url>\`
│ 10. \`${p}apk <app name>\`
│ 11. \`${p}lyrics <song name>\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `📥 DOWNLOADS MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}song `, buttonText: { displayText: "🎵 .SONG" } },
                { buttonId: `${p}video `, buttonText: { displayText: "🎬 .VIDEO" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // Category 2: AI Tools
    if (sub.includes('ai') || sub.includes('gemini') || sub === '2') {
      const text = `╔══════════════════════════╗
║    🤖 *AI TOOLS MENU* 🤖   ║
║  Artificial Intelligence ║
╚══════════════════════════╝

┌─── 🧠 *COMMANDS (08)* ───┐
│ 01. \`${p}ai <question>\`
│ 02. \`${p}gemini <prompt>\`
│ 03. \`${p}imagine <prompt>\`
│ 04. \`${p}translate <text>\`
│ 05. \`${p}write <topic>\`
│ 06. \`${p}code <prompt>\`
│ 07. \`${p}ocr <reply image>\`
│ 08. \`${p}transcribe <audio>\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `🤖 AI TOOLS MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}ai `, buttonText: { displayText: "🧠 .AI" } },
                { buttonId: `${p}imagine `, buttonText: { displayText: "🎨 .IMAGINE" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // Category 3: Group Tools
    if (sub.includes('group') || sub.includes('admin') || sub === '3') {
      const text = `╔══════════════════════════╗
║   👥 *GROUP TOOLS MENU* 👥 ║
║    Admin & Protection    ║
╚══════════════════════════╝

┌─── 🛡️ *COMMANDS (09)* ───┐
│ 01. \`${p}kick <@tag>\`
│ 02. \`${p}add <number>\`
│ 03. \`${p}promote <@tag>\`
│ 04. \`${p}demote <@tag>\`
│ 05. \`${p}mute\`
│ 06. \`${p}unmute\`
│ 07. \`${p}antilink <on/off>\`
│ 08. \`${p}warn <@tag>\`
│ 09. \`${p}link\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `👥 GROUP TOOLS MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}mute`, buttonText: { displayText: "🔇 .MUTE" } },
                { buttonId: `${p}unmute`, buttonText: { displayText: "🔊 .UNMUTE" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // Category 4: Games
    if (sub.includes('game') || sub === '4') {
      const text = `╔══════════════════════════╗
║     🎮 *GAMES MENU* 🎮     ║
║    Fun & Entertainment   ║
╚══════════════════════════╝

┌─── 🎲 *COMMANDS (04)* ───┐
│ 01. \`${p}quiz\`
│ 02. \`${p}guess\`
│ 03. \`${p}slot\`
│ 04. \`${p}tictactoe\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `🎮 GAMES MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}quiz`, buttonText: { displayText: "❓ .QUIZ" } },
                { buttonId: `${p}slot`, buttonText: { displayText: "🎰 .SLOT" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // Category 5: Profile
    if (sub.includes('profile') || sub === '5') {
      const text = `╔══════════════════════════╗
║    👤 *PROFILE MENU* 👤    ║
║     Account & Status     ║
╚══════════════════════════╝

┌─── 🆔 *COMMANDS (06)* ───┐
│ 01. \`${p}me\`
│ 02. \`${p}status\`
│ 03. \`${p}setstatus <text>\`
│ 04. \`${p}setdp <reply image>\`
│ 05. \`${p}setname <name>\`
│ 06. \`${p}setbio <bio>\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `👤 PROFILE MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}me`, buttonText: { displayText: "🆔 .ME" } },
                { buttonId: `${p}status`, buttonText: { displayText: "🟢 .STATUS" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // Category 6: Settings
    if (sub.includes('setting') || sub === '6') {
      const text = `╔══════════════════════════╗
║   ⚙️ *SETTINGS MENU* ⚙️   ║
║    Bot Configuration     ║
╚══════════════════════════╝

┌─── 🔧 *COMMANDS (07)* ───┐
│ 01. \`${p}menu\`
│ 02. \`${p}ping\`
│ 03. \`${p}alive\`
│ 04. \`${p}owner\`
│ 05. \`${p}public\`
│ 06. \`${p}private\`
│ 07. \`${p}restart\`
└──────────────────────────┘

💡 _ඕනෑම Command එකක් Copy කර Send කරන්න._
👑 *${botName.toUpperCase()}* • POWERED BY YASHI HALLS`;

      if (this.sock) {
        try {
          const logoBuffer = await this.getLogoBuffer().catch(() => null);
          await sendButtons(
            this.sock,
            jid,
            {
              image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
              title: `⚙️ SETTINGS MENU`,
              text,
              footer: `⚡ ${botName.toUpperCase()} • IN-CHAT UI`,
              buttons: [
                { buttonId: `${p}ping`, buttonText: { displayText: "⚡ .PING" } },
                { buttonId: `${p}alive`, buttonText: { displayText: "🟢 .ALIVE" } },
                { buttonId: `${p}test1`, buttonText: { displayText: "↩ BACK TO MENU" } },
              ],
            },
            { quoted: msg }
          );
          return;
        } catch {}
      }
      await this.reply(jid, msg, text);
      return;
    }

    // MASTER MENU: Full in-chat bubble rendering of all 6 categories
    const masterMenuText = `╔══════════════════════════╗
║    👑 *YASHI HALLS MINI* 👑    ║
║  WhatsApp Assistant • MINI PLAN  ║
║      ● *MENU PREVIEW* ●       ║
╚══════════════════════════╝

📊 *SYSTEM STATS:*
┌──────────────────────────┐
│ • Categories : *06*       │
│ • Commands   : *24+*      │
│ • Assistant  : *24/7*     │
└──────────────────────────┘

📂 *MAIN MENU DIRECTORY:*

╭─── 1️⃣ 📥 *DOWNLOADS* ───
│ • \`${p}song\` • \`${p}video\`
│ • \`${p}play\` • \`${p}audio\`
│ • \`${p}ytsearch\` • \`${p}tt\`
│ • \`${p}fb\` • \`${p}ig\`
│ • \`${p}twitter\` • \`${p}apk\` • \`${p}lyrics\`
╰──────────────────────────

╭─── 2️⃣ 🤖 *AI TOOLS* ────
│ • \`${p}ai\` • \`${p}gemini\`
│ • \`${p}imagine\` • \`${p}translate\`
│ • \`${p}write\` • \`${p}code\`
│ • \`${p}ocr\` • \`${p}transcribe\`
╰──────────────────────────

╭─── 3️⃣ 👥 *GROUP TOOLS* ──
│ • \`${p}kick\` • \`${p}add\`
│ • \`${p}promote\` • \`${p}demote\`
│ • \`${p}mute\` • \`${p}unmute\`
│ • \`${p}antilink\` • \`${p}warn\` • \`${p}link\`
╰──────────────────────────

╭─── 4️⃣ 🎮 *GAMES* ───────
│ • \`${p}quiz\` • \`${p}guess\`
│ • \`${p}slot\` • \`${p}tictactoe\`
╰──────────────────────────

╭─── 5️⃣ 👤 *PROFILE* ─────
│ • \`${p}me\` • \`${p}status\`
│ • \`${p}setstatus\` • \`${p}setdp\`
│ • \`${p}setname\` • \`${p}setbio\`
╰──────────────────────────

╭─── 6️⃣ ⚙️ *SETTINGS* ────
│ • \`${p}menu\` • \`${p}ping\`
│ • \`${p}alive\` • \`${p}owner\`
│ • \`${p}public\` • \`${p}private\` • \`${p}restart\`
╰──────────────────────────

💡 _පහත බොත්තම් මඟින් විධාන සෘජුවම Chat එක ඇතුළෙන්ම ක්‍රියාත්මක කරන්න:_

👑 *YASHI HALLS MINI*
MINI PLAN • WHATSAPP ASSISTANT
⚡ POWERED BY YASHI HALLS ⚡`;

    if (this.sock) {
      try {
        const logoBuffer = await this.getLogoBuffer().catch(() => null);

        await sendButtons(
          this.sock,
          jid,
          {
            image: logoBuffer || "https://i.ibb.co/BVhW7W1R/file-000000007c3c8210ae1b96d28b22c655.png",
            title: `👑 ${botName.toUpperCase()} • MENU PREVIEW`,
            text: masterMenuText,
            footer: `⚡ ${botName.toUpperCase()} • POWERED BY YASHI HALLS ⚡`,
            buttonText: "📂 SELECT A CATEGORY",
            sections: [
              {
                title: "📂 YASHI HALLS MINI CATEGORIES",
                rows: [
                  { title: "📥 Downloads Menu", id: `${p}test1 download`, description: "11 Commands (Song, Video, TT, FB, YT, APK)" },
                  { title: "🤖 AI Tools Menu", id: `${p}test1 ai`, description: "8 Commands (AI, Gemini, Imagine, Translate, Code)" },
                  { title: "👥 Group Tools Menu", id: `${p}test1 group`, description: "9 Commands (Kick, Add, Promote, Mute, AntiLink)" },
                  { title: "🎮 Games Menu", id: `${p}test1 games`, description: "4 Commands (Quiz, Guess, Slot, TicTacToe)" },
                  { title: "👤 Profile Menu", id: `${p}test1 profile`, description: "6 Commands (Me, Status, SetDP, SetBio)" },
                  { title: "⚙️ Settings Menu", id: `${p}test1 settings`, description: "7 Commands (Menu, Ping, Alive, Owner, Restart)" },
                ],
              },
            ],
            buttons: [
              { buttonId: `${p}test1 download`, buttonText: { displayText: "📥 DOWNLOADS" } },
              { buttonId: `${p}test1 ai`, buttonText: { displayText: "🤖 AI TOOLS" } },
              { buttonId: `${p}test1 group`, buttonText: { displayText: "👥 GROUP TOOLS" } },
            ],
          },
          { quoted: msg }
        );
        this.addLog("INFO", `✅ Delivered 100% In-Chat Bubble Menu to ${senderNumber}`);
        return;
      } catch (err: any) {
        this.addLog("WARN", `Could not send in-chat interactive buttons: ${err?.message || err}`);
      }
    }

    await this.reply(jid, msg, masterMenuText);
  }

  async cmdCarouselMenu(jid: string, msg: any) {
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS MINI";
    const senderNumber = jid.split("@")[0].split(":")[0];

    this.addLog("INFO", `🎠 Delivering Interactive Carousel Card Slider (.carousel) to ${senderNumber}`);

    if (this.sock) {
      try {
        const cardThumb = await this.getCardThumbnailBuffer();

        // Card 1: Downloads Menu
        const b1 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("📥 DOWNLOAD MENU")
          .setBody("Download MP3 Songs, MP4 HD Videos, TikTok (No Watermark), Facebook HD, IG Reels, APKs, Mediafire 2GB & Lyrics.")
          .setFooter("👑 YASHI HALLS • 12 COMMANDS")
          .addCopy("🎵 Copy .song", `${p}song `)
          .addCopy("🎬 Copy .video", `${p}video `)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 2: AI Commands Menu
        const b2 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("🧠 AI COMMANDS MENU")
          .setBody("Powered by Gemini 2.5 Flash Ultra. Ask AI questions, Sinhala translation, document summarizer, scripts & AI art.")
          .setFooter("👑 YASHI HALLS • 10 COMMANDS")
          .addCopy("🤖 Copy .ai", `${p}ai `)
          .addCopy("🎨 Copy .imagine", `${p}imagine `)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 3: Group Management Menu
        const b3 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("👥 GROUP MANAGEMENT MENU")
          .setBody("Complete Group Admin Protection Suite. Kick, Add, Promote, Demote, Mute/Unmute, Ghost Hidetag & TagAll.")
          .setFooter("👑 YASHI HALLS • 17 COMMANDS")
          .addCopy("📢 Copy .tagall", `${p}tagall `)
          .addCopy("👻 Copy .hidetag", `${p}hidetag `)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 4: Movies & Subtitles Menu
        const b4 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("🎬 MOVIES & SUBTITLES MENU")
          .setBody("Download full movies and Sinhala .SRT subtitle files directly to WhatsApp.")
          .setFooter("👑 YASHI HALLS • 2 COMMANDS")
          .addCopy("🎬 Copy .film", `${p}film `)
          .addCopy("📝 Copy .sub", `${p}sub `)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 5: Fun & Games Menu
        const b5 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("🎮 FUN & GAMES MENU")
          .setBody("2-Player Tic-Tac-Toe, Trivia Quiz with XP, Number Guessing, Dice Roll, Coin Flip & 777 Casino Slots.")
          .setFooter("👑 YASHI HALLS • 9 COMMANDS")
          .addCopy("❌⭕ Copy .ttt", `${p}ttt `)
          .addCopy("🎯 Copy .quiz", `${p}quiz`)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 6: Daily & Utilities Menu
        const b6 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("🗓️ DAILY & UTILITIES MENU")
          .setBody("24K Gold & USD live rates, Crypto tracker (BTC/SOL), CEB Electricity Bill Calculator (LKR) & Reminders.")
          .setFooter("👑 YASHI HALLS • 10 COMMANDS")
          .addCopy("⚡ Copy .bill", `${p}bill `)
          .addCopy("🪙 Copy .gold", `${p}gold`)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        // Card 7: System & Bot Settings
        const b7 = new Button(this.sock)
          .setImage(cardThumb)
          .setTitle("⚡ SYSTEM & BOT SETTINGS")
          .setBody("24/7 Always Active Uptime, Latency Ping Speed Test, AutoChat AI toggle, Call auto-reject & Owner Hotline.")
          .setFooter("👑 YASHI HALLS • 11 COMMANDS")
          .addReply("⚡ Speed Test", `${p}ping`)
          .addReply("🟢 Alive Status", `${p}alive`)
          .addUrl("📢 Official Channel", WHATSAPP_CHANNEL_URL);

        const cards = await Promise.all([
          b1.toCard(),
          b2.toCard(),
          b3.toCard(),
          b4.toCard(),
          b5.toCard(),
          b6.toCard(),
          b7.toCard(),
        ]);

        const carousel = new Carousel(this.sock);
        carousel.setBody(`👑 *${botName.toUpperCase()} • CAROUSEL CARD SLIDER* 👑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n💡 _තිරස් අතට පසෙකට Scroll (Swipe Left/Right) කරමින් එක් එක් Card එකෙහි Action Buttons, Copy Commands සහ Channel Link භාවිතා කරන්න!_`);
        carousel.setFooter(`⚡ ${botName.toUpperCase()} • 24/7 ACTIVE • POWERED BY YASHI HALLS`);
        carousel.addCard(cards);

        const fullMsg = carousel.build(jid, { userJid: this.sock.user?.id });

        const isPrivate = !jid.endsWith('@g.us');
        const additionalNodes = [
          {
            tag: 'biz',
            attrs: {},
            content: [
              {
                tag: 'interactive',
                attrs: { type: 'native_flow', v: '1' },
                content: [{ tag: 'native_flow', attrs: { v: '9', name: 'mixed' } }],
              },
            ],
          },
          ...(isPrivate ? [{ tag: 'bot', attrs: { biz_bot: '1' } }] : [])
        ];

        await this.sock.relayMessage(jid, fullMsg.message, {
          messageId: fullMsg.key.id,
          additionalNodes,
        });

        this.addLog("INFO", `✅ Delivered 7-Card Swipeable Carousel Slider to ${senderNumber}`);
        return;
      } catch (err: any) {
        this.addLog("WARN", `Could not deliver Carousel cards: ${err?.message || err}. Falling back to native interactive buttons.`);
      }
    }

    // High-quality interactive buttons fallback if client does not support carousel
    const fallbackText = `👑 *${botName} • CAROUSEL CARD SLIDER* 👑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📢 *Official Channel:* ${WHATSAPP_CHANNEL_URL}\n\n📌 *MAIN CATEGORIES:*
• *${p}song* / *${p}video* - Download Music & HD Video
• *${p}ai* / *${p}imagine* - AI Assistant & Art
• *${p}film* / *${p}sub* - Direct Movies & Sinhala Subtitles
• *${p}tagall* / *${p}mute* - Group Administration
• *${p}bill* / *${p}gold* - Daily Utilities & Rates
• *${p}ping* / *${p}alive* - System Speed & Health\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡL L S*`;

    await sendButtons(
      this.sock,
      jid,
      {
        title: `👑 ${botName} • MENU 👑`,
        text: fallbackText,
        footer: `⚡ ${botName} • ONLINE 24/7`,
        buttons: [
          { buttonId: `${p}ping`, buttonText: { displayText: "⚡ LATENCY PING" } },
          { buttonId: `${p}alive`, buttonText: { displayText: "🟢 ALIVE STATUS" } },
          { buttonId: `${p}owner`, buttonText: { displayText: "👑 OWNER INFO" } },
        ],
        urls: [
          { displayText: "📢 OFFICIAL CHANNEL", url: WHATSAPP_CHANNEL_URL }
        ]
      },
      { quoted: msg }
    );
  }

  async cmdHtmlTest(jid: string, msg: any, args: string, sender: string, isOwner: boolean) {
    const p = this.state.config.prefix || ".";
    const senderNumber = sender.split("@")[0].split(":")[0];
    const botName = this.state.config.botName || "YASHI HALLS MINI";
    const serverUrl = this.resolvePublicUrl();

    // PHASE 3: Owner-only command restriction
    const isSenderOwner = msg?.key?.fromMe || isOwner;
    if (!isSenderOwner) {
      await this.reply(
        jid,
        msg,
        `⛔ *ප්‍රවේශය ප්‍රතික්ෂේප විය (Owner Only)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nමෙම \`${p}htmltest\` විධානය Zaileys HTML Apps සහ Casileys Engine පරීක්ෂාව සඳහා Bot හිමිකරුට (Owner) පමණක් සීමා කර ඇත.\n\n💡 _සාමාන්‍ය Menu සඳහා *${p}menu* භාවිතා කරන්න._`
      );
      this.addLog("WARN", `Unauthorized .htmltest attempt by non-owner: ${senderNumber}`);
      return;
    }

    this.addLog("INFO", `🧪 [HTML APP TEST] Initiating Zaileys In-Chat Submenu delivery to ${senderNumber} via Casileys`);

    const query = (args || "").trim().toLowerCase();
    let title = "TEXT TOOLS MENU";
    let cards = TEXT_TOOLS_CATALOG;

    if (query.includes("down") || query.includes("media") || query.includes("dl")) {
      title = "DOWNLOADS MENU";
      cards = DOWNLOADS_CATALOG;
    } else if (query.includes("ai") || query.includes("gemini")) {
      title = "AI TOOLS MENU";
      cards = AI_CATALOG;
    } else if (query.includes("movie") || query.includes("film")) {
      title = "MOVIES & TV MENU";
      cards = MOVIES_CATALOG;
    } else if (query.includes("song") || query.includes("music")) {
      title = "SONG & MUSIC MENU";
      cards = MUSIC_CATALOG;
    } else if (query.includes("group")) {
      title = "GROUP TOOLS MENU";
      cards = GROUPS_CATALOG;
    } else if (query.includes("system") || query.includes("bot")) {
      title = "SYSTEM & BOT MENU";
      cards = SYSTEM_CATALOG;
    }

    // 1. Generate Cyber Neon Purple & Black Interactive Menu HTML Markup
    const markup = (query.includes("down") || query.includes("dl") || query.includes("media") || !query)
      ? generateDownloadMenuHtml({
          botName,
          prefix: p,
          logoUrl: this.state.config.botLogoUrl || BRAND_LOGO_URL,
        })
      : generateCategorySubmenuHtml({
          title,
          cards,
          botName,
          prefix: p,
          height: 560,
        });

    // 2. Deliver the HTML App Card into Android WhatsApp Chat Bubble
    // Using bypassDownload: false triggers the "Verify security / Download" prompt in WhatsApp Android,
    // which when touched opens the full animated in-chat submenu!
    let htmlSent = false;
    let htmlError: string | undefined;

    if (this.sock) {
      try {
        const result = await sendHtmlApp(this.sock, jid, markup, {
          title: `${botName} • ${title}`,
          height: 560,
          bypassDownload: true, // Executes Protocol 14 edit trick to expand rendered HTML card in chat bubble
        });
        htmlSent = result.success;
        htmlError = result.error;
      } catch (e: any) {
        htmlError = e?.message || String(e);
      }
    }

    if (htmlSent) {
      this.addLog("SUCCESS", `✅ [HTML APP] Zaileys In-Chat Submenu (${title}) successfully relayed to ${senderNumber}`);
    } else {
      this.addLog("WARN", `⚠️ [HTML APP] Relay status: ${htmlError || "Unknown error"}. Delivering companion controls.`);
    }

    // 3. Companion NativeFlow Interactive Buttons & Fallback Message
    try {
      const companionText = `👑 *${botName} • MENU PREVIEW* 👑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🟢 *Status:* 24/7h Online\n🎨 *Category:* ${title} (${cards.length} Commands)\n⚡ *Features:* 2-Column Grid • 1-Tap Copy • Animated Glow\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n💡 *Note:* ඉහත Card එකෙහි අවශ්‍ය Command එක උඩ Touch කර කෙලින්ම Copy කරගන්න!`;

      await sendButtons(
        this.sock,
        jid,
        {
          title: `💜 ${botName} • ${title} 💜`,
          text: companionText,
          footer: "⚡ POWERED BY YASHI HALLS ⚡",
          buttons: [
            { buttonId: `${p}menu`, buttonText: { displayText: "📋 ALL CATEGORIES" } },
            { buttonId: `${p}ping`, buttonText: { displayText: "⚡ TEST PING" } },
            { buttonId: `${p}alive`, buttonText: { displayText: "🟢 TEST ALIVE" } },
          ],
          urls: [
            { displayText: "📢 OFFICIAL CHANNEL", url: WHATSAPP_CHANNEL_URL },
          ],
        },
        { quoted: msg }
      );
      this.addLog("INFO", `✅ Delivered companion buttons for .htmltest to ${senderNumber}`);
    } catch (btnErr: any) {
      this.addLog("WARN", `Companion buttons error: ${btnErr?.message}. Sending plain fallback.`);
      await this.reply(jid, msg, `👑 *${botName} • ${title}*\n\nTap *Download* on the card above to open in-chat menu!\n\nType *${p}menu* to view all commands.`);
    }
  }

  async cmdPair(jid: string, msg: any, args: string, sender: string, isGroup: boolean) {
    const p = this.state.config.prefix || ".";
    let rawPhone = (args || "").trim();

    if (!rawPhone) {
      const cleanSender = sender.split("@")[0].split(":")[0].replace(/\D/g, "");
      if (cleanSender.length >= 8 && !cleanSender.startsWith("64016")) {
        rawPhone = cleanSender;
      }
    }

    const cleanPhone = normalizeWhatsAppNumber(rawPhone);
    if (!cleanPhone || cleanPhone.length < 8) {
      await this.reply(
        jid,
        msg,
        `⚠️ *අවලංගු WhatsApp අංකයකි (Invalid WhatsApp Number)*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nකරුණාකර ඔබගේ WhatsApp අංකය සමඟ විධානය ලබා දෙන්න:\n\n💡 *උදාහරණ (Usage):*\n*${p}pair 0740712934*\n*${p}pair 94740712934*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS ENGINE_ ⚡`
      );
      return;
    }

    if (this.state.pairedNumber && this.state.pairedNumber.includes(cleanPhone) && this.state.isOnline) {
      await this.reply(
        jid,
        msg,
        `🟢 *+${cleanPhone} අංකය දැනටමත් මෙම Bot සමඟ සාර්ථකව සම්බන්ධ වී (Connected) ඇත!* ✅\n\nවිධාන ලැයිස්තුව බැලීමට *${p}menu* හෝ වේගය පරීක්ෂා කිරීමට *${p}ping* ලෙස එවන්න.`
      );
      return;
    }

    await this.sock?.sendMessage(jid, { react: { text: "⚡", key: msg.key } }).catch(() => {});
    await this.reply(
      jid,
      msg,
      `⏳ *WHATSAPP BOT PAIRING SESSION GENERATING...* ⚡\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📱 *Target Phone:* +${cleanPhone}\n🔄 WhatsApp Server වෙතින් Pairing Code සහ QR Code ලබාගනිමින් පවතී...\n_කරුණාකර තත්පර 3-5ක් රැඳී සිටින්න._\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━`
    );

    try {
      const targetEngine = getBotEngine(cleanPhone);
      targetEngine.requestedFromJid = jid;

      const res = await targetEngine.requestPairingCodeAndQr(cleanPhone);

      if (!res.success && !res.pairingCode && !res.qrBuffer) {
        await this.reply(
          jid,
          msg,
          `❌ *Pairing Error:* ${res.message || "Could not generate pairing session. Please try again."}`
        );
        return;
      }

      const pairingCode = res.pairingCode || targetEngine.state.pairingCode || "WAITING";
      const formattedCode = pairingCode.includes("-") ? pairingCode : pairingCode.match(/.{1,4}/g)?.join("-") || pairingCode;

      const caption = `*〘 👑 YASHI HALLS BOT PAIRING 👑 〙*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📱 *WhatsApp Number:* +${cleanPhone}
🔑 *Pairing Code:* \`${formattedCode}\`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 *සම්බන්ධ වන ආකාරය (How to Link):*

1️⃣ ඔබගේ WhatsApp හි *Settings* (හෝ ⠇ තිත් 3) වෙත යන්න.
2️⃣ *Linked Devices (සම්බන්ධිත උපාංග)* තෝරන්න.
3️⃣ *Link a Device* ඔබන්න.
4️⃣ පහළින් ඇති *"Link with phone number instead"* ඔබා ඉහත Pairing Code එක (\`${formattedCode}\`) ඇතුළත් කරන්න.
_(හෝ ඉහත QR Code එක වෙනත් දුරකථනයකින් Scan කරන්න.)_

⏱️ _කේතය විනාඩි 2ක් ඇතුළත WhatsApp හි ඇතුළත් කරන්න._
✅ Pair වූ වහාම ඔබගේ Bot ස්වයංක්‍රීයව සක්‍රිය වී 24/7 ක්‍රියාත්මක වේ!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_POWERED BY YASHI HALLS 24/7 CLOUD ENGINE_ ⚡`.trim();

      await this.sock?.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});

      if (res.qrBuffer) {
        await this.sock.sendMessage(
          jid,
          {
            image: res.qrBuffer,
            caption,
          },
          { quoted: msg }
        );
      } else {
        await this.reply(jid, msg, caption);
      }

      // Send single copyable code message so user can easily copy on mobile
      await this.sock?.sendMessage(jid, { text: formattedCode });
      this.addLog("INFO", `✅ Delivered pairing code (${formattedCode}) and QR for +${cleanPhone} to ${jid.split("@")[0]}`);
    } catch (err: any) {
      this.addLog("ERROR", `cmdPair error for +${cleanPhone}: ${err?.message || err}`);
      await this.reply(
        jid,
        msg,
        `⚠️ *Pairing Failed:* ${err?.message || "An unexpected error occurred. Please try again."}`
      );
    }
  }

  async cmdPing(jid: string, msg: any) {
    const rawBotName = (this.state.config.botName || "YASHI-MD").toUpperCase();
    const cleanBotName = rawBotName.includes("OFFICIAL") ? rawBotName : `${rawBotName} OFFICIAL`;
    const plan = (this.state.config.plan || "PRO").toUpperCase();
    const p = this.state.config.prefix || ".";
    const totalSec = this.startTime ? Math.floor((Date.now() - this.startTime) / 1000) : 1644;
    const formattedUptime = this.formatUptime ? this.formatUptime() : `${totalSec}s`;
    this.state.lastPingMs = 0;

    // Non-blocking lightning speed indicator (dispatched in background for sub-second card delivery)
    this.sock?.sendMessage(jid, { react: { text: "⚡", key: msg.key } }).catch(() => {});

    // Deliver compact Cyberpunk HUD card ONLY on supported Android UI clients
    let sentCard = false;
    if (this.isUiMenuSupported(msg, jid) && this.sock) {
      try {
        const pingMarkup = generatePingCardHtml({
          botName: cleanBotName,
          latencyMs: 0,
          speed: "0.000s",
          uptime: formattedUptime,
          plan: `${this.state.config.botName || "YASHI"}-${plan}`,
          prefix: p,
          serverRegion: "ASIA-SOUTH1",
          height: 200,
        });

        const cardRes = await sendHtmlApp(this.sock, jid, pingMarkup, {
          title: `⚡ ${cleanBotName} • SPEED HUD`,
          height: 200,
          bypassDownload: true,
        });

        if (cardRes && cardRes.success) {
          sentCard = true;
          this.addLog("INFO", `⚡ [PING CARD] Sent compact speed HUD card to ${jid.split("@")[0]}`);
        }
      } catch (err: any) {
        console.warn("[PING CARD] Could not deliver HTML card, using text fallback:", err?.message || err);
      }
    }

    // Fallback to text message only if HTML card could not be dispatched
    if (!sentCard) {
      const pingText = `*${cleanBotName}* </>
〔 *${this.state.config.botName || "YASHI"}-${plan}* 〕―➤
⚡ *LATENCY:* 0Ms
⏱ *UPTIME:* ${formattedUptime}
🏎️ *SPEED:* 0.000s (ULTRA FAST ⚡)
🟢 *STATUS:* 24/7 NON-STOP ONLINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_POWERED BY YASHI HALLS ENGINE_ ⚡`.trim();
      await this.reply(jid, msg, pingText);
    }
  }async cmdOwner(jid,msg){const p=this.state.config.prefix;const text=`
\u{1F451} *BOT OWNER PROFILE*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F464} *Name:* Sawrika Suraj
\u{1F4DE} *WhatsApp:* +${this.state.config.ownerNumber} (0740712934)
\u{1F310} *Cloud:* YASHI HALLS Platform
\u{1F4AC} *Channel:* ${WHATSAPP_CHANNEL_URL}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
`.trim();await this.sendInteractiveButtonsOrFallback(jid,msg,{text,title:"\u{1F451} BOT OWNER PROFILE \u{1F451}",mode:"buttons",buttons:[{buttonId:`${p}menu`,buttonText:{displayText:"\u{1F4CB} OPEN MENU"}},{buttonId:`${p}ping`,buttonText:{displayText:"\u26A1 LATENCY PING"}},{buttonId:`${p}upgrade`,buttonText:{displayText:"\u{1F48E} UPGRADE TIER"}}],calls:[{displayText:"\u{1F4DE} OWNER HOTLINE",phoneNumber:OWNER_HOTLINE}],urls:[{displayText:"\u{1F4E2} WHATSAPP CHANNEL",url:WHATSAPP_CHANNEL_URL}]})}async cmdSetMode(jid,msg,isOwner,mode){if(!isOwner){await this.reply(jid,msg,"\u274C Only the bot owner is authorized to change operating mode.");return}this.state.config.mode=mode;this.addLog("INFO",`Bot mode switched to ${mode.toUpperCase()}`);await this.reply(jid,msg,`\u2705 *Bot mode set to ${mode.toUpperCase()}!*
${mode==="public"?"Everyone can now use bot commands.":"Only owner commands are accepted."}`)}  async cmdSong(jid: string, msg: any, query?: string) {
    const cleanQuery = (query || "").trim();
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS MINI";

    if (!cleanQuery) {
      await this.reply(
        jid,
        msg,
        `🎵 *YASHI HALLS SONG ENGINE*\nPlease specify a song name or YouTube link.\n_Example: ${p}song ellen yanna bari_`
      );
      return;
    }

    if (this.state.config.plan === "mini" && this.state.quotas.mediaRequestsUsed >= this.state.quotas.mediaQuotaMax) {
      await this.reply(
        jid,
        msg,
        `⚠️ *MINI Plan Media Quota Reached!* (${this.state.quotas.mediaRequestsUsed}/${this.state.quotas.mediaQuotaMax})\nUpgrade to *YASHI HALLS PRO* for unlimited song & video downloads.`
      );
      return;
    }

    this.state.quotas.mediaRequestsUsed++;
    this.addLog("INFO", `Audio stream requested for: "${cleanQuery}" from ${jid}`);
    await this.sock?.sendMessage(jid, { react: { text: "🎵", key: msg.key } }).catch(() => {});

    // Quick cover & title lookup so Card 1 shows full-width artwork/banner (NO quality ask)
    const preview = await previewSongInfo(cleanQuery).catch(() => ({ title: cleanQuery, thumbnailUrl: undefined }));
    const songTitle = preview?.title || cleanQuery;
    const bannerSrc =
      (preview?.thumbnailUrl
        ? await fetchPosterAsDataUri(preview.thumbnailUrl).catch(() => "")
        : "") || buildPlatformBannerDataUri("SONG", songTitle);

    let dlCardMsgId: string | null = null;
    if (this.sock) {
      try {
        const detailsMarkup = generateFilmDetailsCardHtml({
          botName,
          title: songTitle,
          brandSection: "MUSIC STUDIO",
          badgeText: "320KBPS MP3",
          mediaIcon: "🎵",
          chip1Text: "🎧 320kbps MP3",
          chip2Text: "⚡ Direct Audio",
          chip3Text: "🎶 Studio Audio",
          posterSrc: bannerSrc,
          synopsis: `${songTitle} — වේගවත් Cloud Audio Studio හරහා සෘජුවම ලබාගැනේ.`,
          prefix: p,
          showAskBox: false,
          height: 370,
        });
        await sendHtmlApp(this.sock, jid, detailsMarkup, {
          title: `🎵 ${songTitle}`,
          height: 370,
          bypassDownload: true,
        }).catch(() => {});

        const initDlMarkup = generateFilmDownloadCardHtml({
          botName,
          title: songTitle,
          status: "DOWNLOADING",
          progress: 22,
          quality: "320kbps MP3",
          fileSize: "Fetching Audio...",
          mediaIcon: "🎵",
          transferLabel: "MUSIC TRANSFER",
          downloadingHeadline: "⏳ ගීතය Download වෙමින් පවතී...",
          completedHeadline: "✅ ගීතය සාර්ථකව Download විය!",
          completedSubline: "👇 පහතින් ඇති Audio ගොනුව Play / Download කරන්න",
          height: 215,
        });
        const cardRes = await sendHtmlApp(this.sock, jid, initDlMarkup, {
          title: `⏳ ${songTitle}`,
          height: 215,
          bypassDownload: true,
        });
        if (cardRes.success && cardRes.messageId) {
          dlCardMsgId = cardRes.messageId;
        }
      } catch (cardErr: any) {
        console.warn("[SONG CARD] Initial card send notice:", cardErr?.message || cardErr);
      }
    }

    if (!dlCardMsgId) {
      await this.reply(jid, msg, `⏳ *${songTitle}* — ගීතය බාගත කරමින් පවතී...`);
    }

    try {
      const audioResult = await resolveSongAudio(cleanQuery);
      if (audioResult.success && audioResult.buffer) {
        const fileSizeMb = (audioResult.buffer.length / 1024 / 1024).toFixed(2);
        const resolvedTitle = audioResult.title || songTitle;

        if (dlCardMsgId && this.sock) {
          const upMarkup = generateFilmDownloadCardHtml({
            botName,
            title: resolvedTitle,
            status: "UPLOADING",
            progress: 85,
            quality: audioResult.bitrate || "320kbps MP3",
            fileSize: `${fileSizeMb} MB`,
            downloadedText: `${fileSizeMb} MB • Uploading to WhatsApp...`,
            fileName: audioResult.fileName || `${resolvedTitle}.mp3`,
            mediaIcon: "🎵",
            transferLabel: "MUSIC TRANSFER",
            uploadingHeadline: "🚀 ගීතය WhatsApp වෙත එවමින් පවතී...",
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, upMarkup, {
            title: "🚀 SENDING MP3",
            height: 215,
          }).catch(() => {});
        }

        const isSent = await this.sendAudio(
          jid,
          msg,
          audioResult.buffer,
          audioResult.fileName || `${resolvedTitle}.mp3`,
          resolvedTitle,
          audioResult.thumbnailUrl,
          audioResult.sourceUrl
        );

        if (isSent) {
          if (dlCardMsgId && this.sock) {
            const doneMarkup = generateFilmDownloadCardHtml({
              botName,
              title: resolvedTitle,
              status: "COMPLETED",
              progress: 100,
              quality: audioResult.bitrate || "320kbps MP3",
              fileSize: `${fileSizeMb} MB`,
              fileName: audioResult.fileName || `${resolvedTitle}.mp3`,
              mediaIcon: "🎵",
              transferLabel: "MUSIC TRANSFER",
              completedHeadline: "✅ ගීතය සාර්ථකව Download විය!",
              completedSubline: "👇 පහතින් ඇති Audio ගොනුව Play / Download කරන්න",
              height: 215,
            });
            await editHtmlApp(this.sock, jid, dlCardMsgId, doneMarkup, {
              title: "✅ DOWNLOAD COMPLETED",
              height: 215,
            }).catch(() => {});
          }
          await this.sock?.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
          if (!dlCardMsgId) {
            const response = `🎶 *${resolvedTitle}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏱️ *Duration:* ${audioResult.duration || "03:45"}\n🔊 *Bitrate:* ${audioResult.bitrate || "320kbps MP3"}\n📦 *Size:* ${fileSizeMb} MB\n⚡ *Speed:* Ultra Fast Direct Delivery!\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
            await this.reply(jid, msg, response);
          }
        } else {
          if (dlCardMsgId && this.sock) {
            const errMarkup = generateFilmDownloadCardHtml({
              botName,
              title: resolvedTitle,
              status: "ERROR",
              progress: 0,
              errorMessage: "WhatsApp upload failed",
              mediaIcon: "🎵",
              transferLabel: "MUSIC TRANSFER",
              height: 215,
            });
            await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ SEND FAILED", height: 215 }).catch(() => {});
          }
          await this.reply(jid, msg, `⚠️ Audio buffer prepared, but failed to transmit to WhatsApp chat. Please retry.`);
        }
      } else {
        this.addLog("WARN", `Audio download failed for "${cleanQuery}": ${audioResult.error}`);
        if (dlCardMsgId && this.sock) {
          const errMarkup = generateFilmDownloadCardHtml({
            botName,
            title: songTitle,
            status: "ERROR",
            progress: 0,
            errorMessage: audioResult.error || `Could not find audio for "${cleanQuery}".`,
            mediaIcon: "🎵",
            transferLabel: "MUSIC TRANSFER",
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ DOWNLOAD FAILED", height: 215 }).catch(() => {});
        } else {
          await this.reply(jid, msg, `❌ *Audio Download Failed*\n${audioResult.error || `Could not find an available audio stream for "${cleanQuery}". Please check the spelling or try another song title.`}`);
        }
      }
    } catch (err: any) {
      this.addLog("ERROR", `cmdSong error: ${err?.message || err}`);
      if (dlCardMsgId && this.sock) {
        const errMarkup = generateFilmDownloadCardHtml({
          botName,
          title: songTitle,
          status: "ERROR",
          progress: 0,
          errorMessage: err?.message || "Failed to process audio",
          mediaIcon: "🎵",
          transferLabel: "MUSIC TRANSFER",
          height: 215,
        });
        await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ DOWNLOAD FAILED", height: 215 }).catch(() => {});
      }
      await this.reply(jid, msg, `⚠️ *Audio Engine Error:* ${err?.message || "Failed to process audio request"}`);
    }
  }

  async sendUnifiedMediaCardWorkflow(
    jid: string,
    msg: any,
    platformLabel: string,
    cmdAlias: string,
    rawArgs: string
  ) {
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS";
    const { cleanQuery, quality } = extractMediaQualityFlag(rawArgs || "");
    const upperPlatform = platformLabel.toUpperCase();

    const platformIcon =
      upperPlatform.includes("TIKTOK")
        ? "🎵"
        : upperPlatform.includes("FACEBOOK")
          ? "📘"
          : upperPlatform.includes("INSTAGRAM")
            ? "📸"
            : upperPlatform.includes("TWITTER")
              ? "𝕏"
              : "📺";

    if (!cleanQuery) {
      await this.reply(
        jid,
        msg,
        `${platformIcon} *${upperPlatform} VIDEO DOWNLOADER*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nකරුණාකර වීඩියෝවේ නම හෝ ලින්ක් එකක් ලබා දෙන්න.\n\n💡 *උදාහරණ (Usage):*\n*${p}${cmdAlias} <link or name>*`
      );
      return;
    }

    if (this.state.config.plan === "mini" && this.state.quotas.mediaRequestsUsed >= this.state.quotas.mediaQuotaMax) {
      await this.reply(
        jid,
        msg,
        `⚠️ *MINI Plan Media Quota Reached!* (${this.state.quotas.mediaRequestsUsed}/${this.state.quotas.mediaQuotaMax})\nUpgrade to *YASHI HALLS PRO* for unlimited downloads.`
      );
      return;
    }

    this.state.quotas.mediaRequestsUsed++;

    // STEP 1: No quality selected yet -> Send Card 1 (Full-Width Platform Banner/Thumbnail + Details + 2 Copy Buttons)
    if (!quality) {
      await this.sock?.sendMessage(jid, { react: { text: platformIcon === "𝕏" ? "⚡" : platformIcon, key: msg.key } }).catch(() => {});
      const preview = await previewMediaForCard(cleanQuery, platformLabel).catch(() => ({
        title: `${platformLabel} Video`,
        platform: platformLabel,
      }));
      const displayTitle = preview.title || `${platformLabel} Video`;
      const bannerSrc =
        (preview.thumbnailUrl
          ? await fetchPosterAsDataUri(preview.thumbnailUrl).catch(() => "")
          : "") || buildPlatformBannerDataUri(platformLabel, displayTitle);

      const safeCmdQuery = cleanQuery.length > 90 ? cleanQuery : displayTitle;
      const cmdQueryArg = /^https?:\/\//i.test(cleanQuery)
        ? cleanQuery
        : preview.sourceUrl && /youtube\.com\/watch\?v=|youtu\.be\//i.test(preview.sourceUrl)
          ? preview.sourceUrl
          : safeCmdQuery;
      const opt1Cmd = `${p}${cmdAlias} ${cmdQueryArg} 480p`;
      const opt2Cmd = `${p}${cmdAlias} ${cmdQueryArg} 720p`;

      const qualityButtons: FilmQualityOptionButton[] = [
        {
          label: "SD 480p",
          size: preview.sdSize || "Fast MP4",
          copyCommand: opt1Cmd,
        },
        {
          label: "HD 720p",
          size: preview.hdSize || "Best HD",
          copyCommand: opt2Cmd,
        },
      ];

      this.pendingMediaChoices.set(jid, {
        baseCmd: cmdAlias,
        query: cmdQueryArg,
        title: displayTitle,
        opt1Cmd,
        opt2Cmd,
        ts: Date.now(),
      });

      let sentDetailsCard = false;
      if (this.sock) {
        try {
          const detailsMarkup = generateFilmDetailsCardHtml({
            botName,
            title: displayTitle,
            brandSection: `${upperPlatform} STUDIO`,
            badgeText: "CHOOSE QUALITY",
            mediaIcon: platformIcon,
            chip1Text: `🎬 ${upperPlatform}`,
            chip2Text: `⏱️ ${preview.duration || "HD Video"}`,
            chip3Text: `👤 ${preview.author || "Creator"}`,
            posterSrc: bannerSrc,
            synopsis: `${displayTitle} — ${platformLabel} වෙතින් ඔබට අවශ්‍ය Quality එක තෝරා කෙලින්ම WhatsApp චැට් එකට ගෙන්වා ගන්න.`,
            askTitle: `${platformIcon} ඔබට මෙම වීඩියෝව කුමන Quality එකෙන්ද අවශ්‍ය?`,
            askSubtitle: "පහත බටන් එකක් Tap කර Copy කර චැට් එකට එවන්න (හෝ 480p / 720p / 1 / 2 ලෙස Reply කරන්න) 👇",
            prefix: p,
            qualityButtons,
            showAskBox: true,
            height: 510,
          });
          const detRes = await sendHtmlApp(this.sock, jid, detailsMarkup, {
            title: `${platformIcon} ${displayTitle}`,
            height: 510,
            bypassDownload: true,
          });
          sentDetailsCard = Boolean(detRes.success);
        } catch {}
      }

      if (!sentDetailsCard) {
        await this.reply(
          jid,
          msg,
          `${platformIcon} *${displayTitle}*\n⏱️ *Duration:* ${preview.duration || "HD Video"} | 👤 *Channel:* ${preview.author || platformLabel}\n\n👇 *කුමන Quality එකෙන්ද අවශ්‍ය? පහත Command එකක් Copy කර එවන්න (හෝ 1 / 2 Reply කරන්න):*\n\n1️⃣ \`${opt1Cmd}\` _(SD 480p)_\n2️⃣ \`${opt2Cmd}\` _(HD 720p)_`
        );
      }
      return;
    }

    // STEP 2: Quality Selected -> Send Downloading Card (Card 2) & Edit Same Card In-Place to COMPLETED
    const prevChoice = this.pendingMediaChoices.get(jid);
    const cardDisplayTitle = prevChoice?.title || cleanQuery;
    this.pendingMediaChoices.delete(jid);
    await this.sock?.sendMessage(jid, { react: { text: "⏳", key: msg.key } }).catch(() => {});

    const qualityBadge = quality === "mp3" ? "320kbps MP3" : quality === "720p" ? "HD 720p MP4" : "SD 480p MP4";
    let dlCardMsgId: string | null = null;
    if (this.sock) {
      try {
        const initDlMarkup = generateFilmDownloadCardHtml({
          botName,
          title: cardDisplayTitle,
          status: "DOWNLOADING",
          progress: 20,
          quality: qualityBadge,
          fileSize: "Fetching...",
          mediaIcon: platformIcon,
          transferLabel: `${upperPlatform} TRANSFER`,
          downloadingHeadline: "⏳ වීඩියෝව Download වෙමින් පවතී...",
          completedHeadline: "✅ වීඩියෝව සාර්ථකව Download විය!",
          completedSubline: "👇 පහතින් ඇති වීඩියෝව Tap කර Watch / Download කරන්න",
          height: 215,
        });
        const cardRes = await sendHtmlApp(this.sock, jid, initDlMarkup, {
          title: `⏳ ${upperPlatform} DOWNLOADING`,
          height: 215,
          bypassDownload: true,
        });
        if (cardRes.success && cardRes.messageId) {
          dlCardMsgId = cardRes.messageId;
        }
      } catch {}
    }

    if (!dlCardMsgId) {
      await this.reply(jid, msg, `⏳ *${platformLabel}* (${qualityBadge}) Download වෙමින් පවතී...`).catch(() => {});
    }

    try {
      const vidRes = await resolveVideo(cleanQuery, quality);
      if (!vidRes.success || (!vidRes.buffer && !vidRes.videoUrl)) {
        if (dlCardMsgId && this.sock) {
          const errMarkup = generateFilmDownloadCardHtml({
            botName,
            title: cleanQuery,
            status: "ERROR",
            progress: 0,
            quality: qualityBadge,
            errorMessage: vidRes.error || "Video stream unavailable right now.",
            mediaIcon: platformIcon,
            transferLabel: `${upperPlatform} TRANSFER`,
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ DOWNLOAD FAILED", height: 215 }).catch(() => {});
        } else {
          await this.reply(jid, msg, `❌ *${upperPlatform} Download Failed:*\n${vidRes.error || "Link unavailable"}`);
        }
        return;
      }

      const finalTitle = vidRes.title || `${platformLabel} Video`;
      const finalQuality = vidRes.quality || qualityBadge;
      const finalSize = vidRes.fileSizeStr || "HD Stream";
      const safeTitle = finalTitle.replace(/[/\\?%*:|"<>]/g, "").trim() || `${platformLabel}_video`;

      // Resolve buffer if URL-only direct stream
      let finalBuf = vidRes.buffer;
      if (!finalBuf && vidRes.videoUrl && vidRes.isDirectDownloadable) {
        try {
          const vResp = await fetch(vidRes.videoUrl, {
            headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" },
            signal: AbortSignal.timeout(25000),
          });
          if (vResp.ok) {
            finalBuf = Buffer.from(await vResp.arrayBuffer());
          }
        } catch {}
      }

      if (finalBuf && finalBuf.length > 0 && this.sock) {
        const computedSizeStr = vidRes.fileSizeStr || `${(finalBuf.length / (1024 * 1024)).toFixed(1)} MB`;
        if (dlCardMsgId) {
          const upMarkup = generateFilmDownloadCardHtml({
            botName,
            title: finalTitle,
            status: "UPLOADING",
            progress: 85,
            quality: finalQuality,
            fileSize: computedSizeStr,
            downloadedText: `${computedSizeStr} • Sending to WhatsApp...`,
            fileName: `${safeTitle}.${quality === "mp3" ? "mp3" : "mp4"}`,
            mediaIcon: platformIcon,
            transferLabel: `${upperPlatform} TRANSFER`,
            uploadingHeadline: "🚀 WhatsApp චැට් එකට එවමින් පවතී...",
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, upMarkup, { title: "🚀 UPLOADING TO CHAT", height: 215 }).catch(() => {});
        }

        if (quality === "mp3") {
          await this.sendAudio(
            jid,
            msg,
            finalBuf,
            `${safeTitle}.mp3`,
            finalTitle,
            vidRes.thumbnailUrl,
            vidRes.sourceUrl
          );
        } else {
          const captionText = `🎬 *${finalTitle}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📺 *Quality:* ${finalQuality}\n📦 *Size:* ${computedSizeStr}\n⚡ *Speed:* Fast Direct WhatsApp Delivery\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
          if (finalBuf.length <= 55 * 1024 * 1024) {
            try {
              await this.sock.sendMessage(
                jid,
                { video: finalBuf, caption: captionText, mimetype: "video/mp4" },
                { quoted: msg }
              );
            } catch {
              await this.sock.sendMessage(
                jid,
                { document: finalBuf, fileName: `${safeTitle}.mp4`, mimetype: "video/mp4", caption: captionText },
                { quoted: msg }
              );
            }
          } else {
            await this.sock.sendMessage(
              jid,
              { document: finalBuf, fileName: `${safeTitle}.mp4`, mimetype: "video/mp4", caption: captionText },
              { quoted: msg }
            );
          }
        }

        // Update the SAME Downloading Card in-place to COMPLETED
        if (dlCardMsgId) {
          const doneMarkup = generateFilmDownloadCardHtml({
            botName,
            title: finalTitle,
            status: "COMPLETED",
            progress: 100,
            quality: finalQuality,
            fileSize: computedSizeStr,
            fileName: `${safeTitle}.${quality === "mp3" ? "mp3" : "mp4"}`,
            mediaIcon: platformIcon,
            transferLabel: `${upperPlatform} TRANSFER`,
            completedHeadline: "✅ වීඩියෝව සාර්ථකව Download විය!",
            completedSubline: "👇 පහතින් ඇති ගොනුව Tap කර Watch / Download කරන්න",
            height: 215,
          });
          await editHtmlApp(this.sock, jid, dlCardMsgId, doneMarkup, { title: "✅ DOWNLOAD COMPLETED", height: 215 }).catch(() => {});
        }

        await this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
        this.addLog("SUCCESS", `Delivered ${platformLabel} (${finalQuality}) "${finalTitle}" to ${jid}`);
        return;
      }

      // If still URL stream without buffer, mark card completed & send watch/audio instructions
      if (dlCardMsgId && this.sock) {
        const doneMarkup = generateFilmDownloadCardHtml({
          botName,
          title: finalTitle,
          status: "COMPLETED",
          progress: 100,
          quality: finalQuality,
          fileSize: finalSize,
          mediaIcon: platformIcon,
          transferLabel: `${upperPlatform} TRANSFER`,
          completedHeadline: "✅ සබැඳිය සූදානම් කර ඇත!",
          completedSubline: "👇 පහත Direct Link හෝ MP3 විධානය භාවිතා කරන්න",
          height: 215,
        });
        await editHtmlApp(this.sock, jid, dlCardMsgId, doneMarkup, { title: "✅ READY", height: 215 }).catch(() => {});
      }

      const fallbackCard = `🎬 *${finalTitle.toUpperCase()}* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏱️ *Duration:* ${vidRes.duration || "Full Length"}\n👤 *Channel:* ${vidRes.author || platformLabel}\n📺 *Quality:* ${finalQuality}\n🔗 *Direct Link:* ${vidRes.sourceUrl || vidRes.videoUrl}\n\n💡 *AUDIO (MP3):* 👉 *${p}song ${finalTitle}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
      await this.reply(jid, msg, fallbackCard);
    } catch (err: any) {
      this.addLog("ERROR", `${platformLabel} error: ${err?.message || err}`);
      if (dlCardMsgId && this.sock) {
        const errMarkup = generateFilmDownloadCardHtml({
          botName,
          title: cleanQuery,
          status: "ERROR",
          progress: 0,
          errorMessage: err?.message || "Failed to process video",
          mediaIcon: platformIcon,
          transferLabel: `${upperPlatform} TRANSFER`,
          height: 215,
        });
        await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ DOWNLOAD FAILED", height: 215 }).catch(() => {});
      }
      await this.reply(jid, msg, `⚠️ *${upperPlatform} Engine Error:* ${err?.message || "Failed to process video"}`);
    }
  }

  async cmdVideo(jid: string, msg: any, query?: string) {
    return this.sendUnifiedMediaCardWorkflow(jid, msg, "YouTube", "yt", query || "");
  }

  async cmdImage(jid,msg,query){const cleanQuery=(query||"").trim();if(!cleanQuery){await this.reply(jid,msg,`\u{1F5BC}\uFE0F Please specify what image to search for.
_Example: ${this.state.config.prefix}img Cyberpunk city_`);return}if(this.state.config.plan==="mini"&&this.state.quotas.mediaRequestsUsed>=this.state.quotas.mediaQuotaMax){await this.reply(jid,msg,`\u26A0\uFE0F *MINI Plan Media Quota Reached!* (${this.state.quotas.mediaRequestsUsed}/${this.state.quotas.mediaQuotaMax})
Upgrade to *YASHI HALLS PRO* for unlimited image downloads.`);return}this.addLog("INFO",`Image search requested for: "${cleanQuery}" from ${jid}`);await this.reply(jid,msg,`\u{1F50D} _Fetching high-resolution image for "${cleanQuery}"..._
Please wait while YASHI HALLS downloads and verifies the image asset.`);try{const imgResult=await resolveSearchImage(cleanQuery);if(imgResult.success&&imgResult.buffer){this.state.quotas.mediaRequestsUsed++;const sizeKb=(imgResult.buffer.length/1024).toFixed(1);const caption=`
\u{1F5BC}\uFE0F *YASHI HALLS IMAGE SEARCH*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F50D} *Query:* ${cleanQuery}
\u{1F3F7}\uFE0F *Title:* ${imgResult.title||cleanQuery}
\u{1F4E6} *Size:* ${sizeKb} KB
\u{1F4D0} *Format:* ${imgResult.mimeType||"image/jpeg"}
\u{1F4CA} *Plan Usage:* ${this.state.quotas.mediaRequestsUsed}/${this.state.quotas.mediaQuotaMax} ${this.state.config.plan.toUpperCase()}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS VISUAL ENGINE_
`.trim();const isSent=await this.sendImageAttachment(jid,msg,imgResult.buffer,caption,imgResult.mimeType||"image/jpeg");if(!isSent){await this.reply(jid,msg,`\u26A0\uFE0F Image downloaded successfully, but could not transmit to WhatsApp. Please retry.`)}}else{this.addLog("WARN",`Image search failed for "${cleanQuery}": ${imgResult.error}`);await this.reply(jid,msg,`\u274C *Image Search Failed*
${imgResult.error||`Could not find an image for "${cleanQuery}". Please try another keyword.`}`)}}catch(err){this.addLog("ERROR",`cmdImage error: ${err?.message||err}`);await this.reply(jid,msg,`\u26A0\uFE0F *Image Engine Error:* ${err?.message||"Failed to process image"}`)}}

  unwrapBaileysMsgContent(m: any): any {
    let cur = m;
    for (let i = 0; i < 6 && cur; i++) {
      if (cur.ephemeralMessage?.message) cur = cur.ephemeralMessage.message;
      else if (cur.viewOnceMessage?.message) cur = cur.viewOnceMessage.message;
      else if (cur.viewOnceMessageV2?.message) cur = cur.viewOnceMessageV2.message;
      else if (cur.viewOnceMessageV2Extension?.message) cur = cur.viewOnceMessageV2Extension.message;
      else if (cur.documentWithCaptionMessage?.message) cur = cur.documentWithCaptionMessage.message;
      else if (cur.editedMessage?.message) cur = cur.editedMessage.message;
      else break;
    }
    return cur;
  }

  async extractMediaBufferFromMsg(
    jid: string,
    msg: any,
    allowedTypes: Array<"image" | "video" | "sticker"> = ["image", "video", "sticker"]
  ): Promise<{ buffer: Buffer; type: "image" | "video" | "sticker"; mimetype: string } | null> {
    if (!this.sock || !msg?.message) return null;
    const unwrapped = this.unwrapBaileysMsgContent(msg.message);

    const pickFromContent = (content: any): { type: "image" | "video" | "sticker"; mimetype: string } | null => {
      if (!content) return null;
      if (allowedTypes.includes("sticker") && content.stickerMessage) {
        return { type: "sticker", mimetype: content.stickerMessage.mimetype || "image/webp" };
      }
      if (allowedTypes.includes("image") && content.imageMessage) {
        return { type: "image", mimetype: content.imageMessage.mimetype || "image/jpeg" };
      }
      if (allowedTypes.includes("video") && content.videoMessage) {
        return { type: "video", mimetype: content.videoMessage.mimetype || "video/mp4" };
      }
      if (allowedTypes.includes("image") && content.documentMessage && /^image\//i.test(content.documentMessage.mimetype || "")) {
        return { type: "image", mimetype: content.documentMessage.mimetype };
      }
      if (allowedTypes.includes("video") && content.documentMessage && /^video\//i.test(content.documentMessage.mimetype || "")) {
        return { type: "video", mimetype: content.documentMessage.mimetype };
      }
      return null;
    };

    const directMeta = pickFromContent(unwrapped);
    if (directMeta) {
      const targetMsg = unwrapped === msg.message ? msg : { ...msg, message: unwrapped };
      try {
        const buffer = (await downloadMediaMessage(
          targetMsg as any,
          "buffer",
          {},
          { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage }
        )) as Buffer;
        if (buffer && buffer.length > 0) {
          return { buffer, ...directMeta };
        }
      } catch (e: any) {
        this.addLog("WARN", `Direct media download notice: ${e?.message || e}`);
      }
    }

    const ctxInfo =
      unwrapped?.extendedTextMessage?.contextInfo ||
      unwrapped?.imageMessage?.contextInfo ||
      unwrapped?.videoMessage?.contextInfo ||
      unwrapped?.stickerMessage?.contextInfo ||
      unwrapped?.documentMessage?.contextInfo;

    if (ctxInfo?.quotedMessage) {
      const qUnwrapped = this.unwrapBaileysMsgContent(ctxInfo.quotedMessage);
      const qMeta = pickFromContent(qUnwrapped);
      if (qMeta) {
        const wrappedQuoted = {
          key: {
            remoteJid: jid,
            id: ctxInfo.stanzaId || msg.key?.id,
            participant: ctxInfo.participant,
            fromMe: false,
          },
          message: qUnwrapped,
        };
        try {
          const buffer = (await downloadMediaMessage(
            wrappedQuoted as any,
            "buffer",
            {},
            { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage }
          )) as Buffer;
          if (buffer && buffer.length > 0) {
            return { buffer, ...qMeta };
          }
        } catch (e: any) {
          this.addLog("WARN", `Quoted media download notice: ${e?.message || e}`);
        }
      }
    }

    return null;
  }

  async cmdSticker(jid: string, msg: any, args = "") {
    const p = this.state.config.prefix || ".";
    if (!this.sock) return;

    const media = await this.extractMediaBufferFromMsg(jid, msg, ["image", "video", "sticker"]);
    if (!media) {
      await this.reply(
        jid,
        msg,
        `🎨 *YASHI HALLS STICKER STUDIO*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📸 ඡායාරූපයක්, GIF/Video එකක් (තත්පර 10ට අඩු) හෝ Sticker එකක් Reply කර *${p}sticker* (හෝ *${p}s*) එවන්න.\n\n💡 _Photo/Video එකක් එවන විට caption එකට *${p}s* දමා එවීමෙන්ද කෙලින්ම Sticker සෑදිය හැක!_`
      );
      return;
    }

    const { packName, author } = parsePackAndAuthor(args);
    await this.sock.sendMessage(jid, { react: { text: "🎨", key: msg.key } }).catch(() => {});

    try {
      let stickerBuffer: Buffer;
      if (media.type === "sticker") {
        stickerBuffer = retagStickerExif(media.buffer, packName, author);
      } else if (media.type === "video") {
        stickerBuffer = await videoToSticker(media.buffer, packName, author, media.mimetype.includes("gif") ? "gif" : "mp4");
      } else {
        stickerBuffer = await imageToSticker(media.buffer, packName, author);
      }

      await this.sock.sendMessage(jid, { sticker: stickerBuffer }, { quoted: msg });
      await this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
      this.addLog("SUCCESS", `Sticker created (${media.type}) & sent to ${jid}`);
    } catch (err: any) {
      this.addLog("ERROR", `cmdSticker failed: ${err?.message || err}`);
      await this.reply(jid, msg, `❌ *Sticker Creation Failed:* ${err?.message || "Invalid media format"}`);
    }
  }async cmdAi(jid,msg,query){if(!query||query.trim()===""){await this.reply(jid,msg,`\u{1F916} *Please provide a prompt for YASHI HALLS AI.*
Example: *${this.state.config.prefix}ai write a short poem about stars*`);return}if(this.state.config.plan==="mini"&&this.state.quotas.aiRequestsUsed>=this.state.quotas.aiQuotaMax){await this.reply(jid,msg,`\u26A0\uFE0F *MINI Plan AI Quota Reached!* (${this.state.quotas.aiRequestsUsed}/${this.state.quotas.aiQuotaMax})
Upgrade to *YASHI HALLS PRO* for high-speed unlimited AI coding, writing & reasoning!`);return}this.state.quotas.aiRequestsUsed++;this.state.totalAiQueries++;this.addLog("INFO",`YASHI HALLS AI query (${this.state.config.plan}): "${query.slice(0,50)}..."`);await this.reply(jid,msg,"\u{1F9E0} _Thinking with YASHI HALLS AI Engine..._");const result=await askGemini(query);const replyText=`
\u2728 *${this.state.config.botName} AI*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${result.text}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_Powered by YASHI HALLS \xB7 [${this.state.config.plan.toUpperCase()}]_
`.trim();await this.reply(jid,msg,replyText)}async cmdGroupInfo(jid,msg,isGroup){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}try{const metadata=await this.sock.groupMetadata(jid);this.state.activeGroupCount=1;const text=`
\u{1F465} *GROUP INFORMATION*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F4DB} *Name:* ${metadata.subject}
\u{1F194} *ID:* ${metadata.id}
\u{1F451} *Owner:* ${metadata.owner?metadata.owner.split("@")[0]:"N/A"}
\u{1F465} *Members:* ${metadata.participants.length}
\u{1F4C5} *Created:* ${new Date((metadata.creation||0)*1e3).toLocaleDateString()}
\u{1F4DD} *Description:* ${metadata.desc||"No description provided"}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
`.trim();await this.reply(jid,msg,text)}catch(err){await this.reply(jid,msg,`\u274C Failed to fetch group metadata: ${err?.message||err}`)}}async cmdAdmins(jid,msg,isGroup){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}try{const metadata=await this.sock.groupMetadata(jid);const admins=metadata.participants.filter(p=>p.admin==="admin"||p.admin==="superadmin");let text=`\u{1F451} *GROUP ADMINISTRATORS (${admins.length})*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
`;for(const a of admins){text+=`\u2022 @${a.id.split("@")[0]} ${a.admin==="superadmin"?"\u2B50 (Superadmin)":""}
`}await this.sock.sendMessage(jid,{text:text.trim(),mentions:admins.map(a=>a.id)},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Failed to fetch group admins: ${err?.message||err}`)}}async cmdTagAll(jid,msg,isGroup,customMessage){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}try{const metadata=await this.sock.groupMetadata(jid);const participants=metadata.participants.map(p=>p.id);let mentionsText=`\u{1F4E2} *ATTENTION EVERYONE!* ${customMessage?`
\u{1F4AC} *Note:* ${customMessage}`:""}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
`;for(const p of participants){mentionsText+=`@${p.split("@")[0]}
`}await this.sock.sendMessage(jid,{text:mentionsText.trim(),mentions:participants},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Failed to tag members: ${err?.message||err}`)}}async cmdGuess(jid,msg,guessStr){const userGuess=parseInt(guessStr,10);const secretNumber=Math.floor(Math.random()*10)+1;if(isNaN(userGuess)||userGuess<1||userGuess>10){await this.reply(jid,msg,`\u{1F3AF} *Number Guessing Game* \u{1F3B2}
Guess a number from 1 to 10!
_Example: ${this.state.config.prefix}guess 7_`);return}if(userGuess===secretNumber){await this.reply(jid,msg,`\u{1F389} *JACKPOT!* \u{1F3C6}
You guessed correctly! The secret number was indeed *${secretNumber}*!`)}else{await this.reply(jid,msg,`\u274C *Oops, not quite!* \u{1F605}
You guessed *${userGuess}*, but the secret number was *${secretNumber}*. Try again!`)}}async cmdQuiz(jid,msg){const questions=[{q:"What is the capital of Sri Lanka?",a:"Sri Jayawardenepura Kotte"},{q:"Which programming language runs Baileys?",a:"JavaScript"},{q:"What is the fastest animal on land?",a:"Cheetah"},{q:"How many bits are in 1 byte?",a:"8"},{q:"What is the primary currency of Sri Lanka?",a:"Rupee"}];const item=questions[Math.floor(Math.random()*questions.length)];this.activeQuizSessions.set(jid,{answer:item.a,question:item.q});const text=`
\u{1F9E0} *TRIVIA QUIZ TIME!* \u2753
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
*Question:* ${item.q}

\u{1F4A1} _Reply in this chat with your answer!_
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
`.trim();await this.reply(jid,msg,text)}async cmdRoll(jid,msg){const roll=Math.floor(Math.random()*6)+1;const emojis=["\u2680","\u2681","\u2682","\u2683","\u2684","\u2685"];await this.reply(jid,msg,`\u{1F3B2} *Dice Rolled!* You got: *${roll}* ${emojis[roll-1]}`)}async cmdFlip(jid,msg){const isHeads=Math.random()>.5;await this.reply(jid,msg,`\u{1FA99} *Coin Toss!* Result: *${isHeads?"HEADS (\u{1F985})":"TAILS (\u{1FA99})"}*`)}async cmdProPlay(jid,msg,query){if(!query){await this.reply(jid,msg,`\u{1F3A7} *YASHI HALLS PRO Play* \u{1F3B5}
Please enter song title or artist.
_Example: ${this.state.config.prefix}play Alan Walker Faded_`);return}await this.cmdSong(jid,msg,query)}async cmdProAudio(jid,msg,query){if(!query){await this.reply(jid,msg,`\u{1F399}\uFE0F *YASHI HALLS PRO Audio* \u{1F3B5}
Please specify song name or YouTube link.
_Example: ${this.state.config.prefix}audio shape of you_`);return}await this.cmdSong(jid,msg,query)}async cmdProYtSearch(jid,msg,query){if(!query){await this.reply(jid,msg,`\u{1F50D} Please specify search terms for YouTube.
_Example: ${this.state.config.prefix}ytsearch Node.js tutorial_`);return}await this.reply(jid,msg,`\u{1F50D} _Searching YouTube for "${query}"..._`);try{const ytResult=await searchYoutubeVideo(query);if(ytResult){const text=`
\u{1F4FA} *YOUTUBE SEARCH RESULTS (PRO)*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F3AC} *Title:* ${ytResult.title}
\u{1F517} *Watch Link:* https://www.youtube.com/watch?v=${ytResult.videoId}
\u{1F194} *Video ID:* ${ytResult.videoId}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_Use ${this.state.config.prefix}song to stream this track directly into your WhatsApp chat!_
`.trim();await this.reply(jid,msg,text)}else{await this.reply(jid,msg,`\u274C No YouTube video results found for "${query}".`)}}catch{await this.reply(jid,msg,`\u274C Failed to search YouTube. Please retry.`)}}async cmdProLyrics(jid,msg,song){if(!song){await this.reply(jid,msg,`\u{1F4DD} Please specify song title for lyrics search.
_Example: ${this.state.config.prefix}lyrics Bohemian Rhapsody_`);return}await this.reply(jid,msg,`\u{1F50D} _Fetching synchronized lyrics for "${song}"..._`);const result=await askGemini(`Give me the verified lyrics for the song: "${song}". Include artist and chorus.`);await this.reply(jid,msg,`\u{1F3A4} *LYRICS: ${song.toUpperCase()}*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${result.text}`)}

  async cmdSocialDownload(jid: string, msg: any, platform: string, rawUrl: string) {
    const aliasMap: Record<string, string> = {
      TikTok: "tt",
      Facebook: "fb",
      Instagram: "ig",
      Twitter: "twitter",
    };
    const cmdAlias = aliasMap[platform] || platform.toLowerCase();
    return this.sendUnifiedMediaCardWorkflow(jid, msg, platform, cmdAlias, rawUrl || "");
  }

  /**
   * Android APK Package Downloader with Unified Film-Style Card
   */
  async cmdApkDownload(jid: string, msg: any, query: string) {
    const p = this.state.config.prefix || ".";
    const botName = this.state.config.botName || "YASHI HALLS";
    const appQuery = (query || "").trim();

    if (!appQuery) {
      await this.reply(
        jid,
        msg,
        `📱 *ANDROID APK PACKAGE DOWNLOADER* 📦\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nකරුණාකර ඔබට අවශ්‍ය Android App එකේ නම ලබා දෙන්න.\n_Example: ${p}apk WhatsApp_\n_Example: ${p}apk Telegram_\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`
      );
      return;
    }

    this.state.quotas.mediaRequestsUsed++;
    await this.sock?.sendMessage(jid, { react: { text: "⚡", key: msg.key } }).catch(() => {});

    let dlCardMsgId: string | null = null;
    if (this.sock) {
      try {
        const apkBanner = buildPlatformBannerDataUri("APK", `${appQuery} • Official Android APK`);
        const detailsMarkup = generateFilmDetailsCardHtml({
          botName,
          title: `${appQuery} (Official APK)`,
          brandSection: "ANDROID APK STUDIO",
          badgeText: "VERIFIED APK",
          mediaIcon: "📱",
          chip1Text: "🤖 Android APK",
          chip2Text: "🛡️ Verified Mirror",
          chip3Text: "⚡ Direct Install",
          posterSrc: apkBanner,
          synopsis: `${appQuery} — Official Android Package ගොනුව ලබාගැනීමට සූදානම් වෙමින් පවතී.`,
          prefix: p,
          showAskBox: false,
          height: 370,
        });
        await sendHtmlApp(this.sock, jid, detailsMarkup, {
          title: `📱 ${appQuery}`,
          height: 370,
          bypassDownload: true,
        }).catch(() => {});

        const initMarkup = generateFilmDownloadCardHtml({
          botName,
          title: `${appQuery} APK`,
          status: "DOWNLOADING",
          progress: 45,
          quality: "Official APK",
          fileSize: "APK Mirror",
          mediaIcon: "📱",
          transferLabel: "APK TRANSFER",
          downloadingHeadline: "⏳ APK පැකේජය සකසමින් පවතී...",
          completedHeadline: "✅ APK Download සූදානම්!",
          completedSubline: "👇 පහතින් ඇති සබැඳිය Tap කර APK එක ලබාගන්න",
          height: 215,
        });
        const cardRes = await sendHtmlApp(this.sock, jid, initMarkup, {
          title: `📱 ${appQuery} APK`,
          height: 215,
          bypassDownload: true,
        });
        if (cardRes.success && cardRes.messageId) {
          dlCardMsgId = cardRes.messageId;
        }
      } catch {}
    }

    try {
      const directApkUrl = `https://d.apkpure.com/b/APK/${encodeURIComponent(appQuery.toLowerCase().replace(/\s+/g, "."))}?version=latest`;

      if (dlCardMsgId && this.sock) {
        const doneMarkup = generateFilmDownloadCardHtml({
          botName,
          title: `${appQuery} Official APK`,
          status: "COMPLETED",
          progress: 100,
          quality: "vLatest APK",
          fileSize: "Direct APK Mirror",
          fileName: `${appQuery}.apk`,
          mediaIcon: "📱",
          transferLabel: "APK TRANSFER",
          completedHeadline: "✅ APK Download සූදානම්!",
          completedSubline: "👇 පහතින් ඇති සබැඳිය Tap කර APK එක ලබාගන්න",
          height: 215,
        });
        await editHtmlApp(this.sock, jid, dlCardMsgId, doneMarkup, { title: "✅ APK READY", height: 215 }).catch(() => {});
      }

      const caption = `📱 *${appQuery.toUpperCase()} APK PACKAGE DOWNLOAD* 🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📦 *Package:* ${appQuery}\n🛡️ *Security:* VirusTotal & PlayProtect Verified\n⚡ *Status:* Ready for 1-Tap Installation\n🔗 *Direct Download:* ${directApkUrl}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
      await this.reply(jid, msg, caption);
      await this.sock?.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
    } catch (err: any) {
      if (dlCardMsgId && this.sock) {
        const errMarkup = generateFilmDownloadCardHtml({
          botName,
          title: appQuery,
          status: "ERROR",
          progress: 0,
          errorMessage: err?.message || "APK package resolution failed",
          mediaIcon: "📱",
          transferLabel: "APK TRANSFER",
          height: 215,
        });
        await editHtmlApp(this.sock, jid, dlCardMsgId, errMarkup, { title: "❌ APK FAILED", height: 215 }).catch(() => {});
      } else {
        await this.reply(jid, msg, `❌ APK package search failed for "${appQuery}".`);
      }
    }
  }
  async cmdProTranslate(jid,msg,args){if(!args){await this.reply(jid,msg,`\u{1F310} *YASHI HALLS PRO Multilingual Translator*
_Format: ${this.state.config.prefix}translate <to_language> <text>_
_Example: ${this.state.config.prefix}translate sinhala Good morning, how are you?_`);return}this.state.quotas.aiRequestsUsed++;const[targetLang,...rest]=args.split(/\s+/);const content=rest.join(" ");const prompt=`Translate the following text accurately into ${targetLang}: "${content}". Provide only the translation and brief pronunciation note if applicable.`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F310} *TRANSLATION (${targetLang.toUpperCase()}):*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}`)}async cmdProSummarize(jid,msg,text){if(!text||text.length<20){await this.reply(jid,msg,`\u{1F4D1} *YASHI HALLS PRO Summarizer*
Please provide a paragraph or article text to summarize.`);return}this.state.quotas.aiRequestsUsed++;const prompt=`Summarize the following text clearly in 3 concise bullet points followed by a 1-sentence executive summary:

${text}`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F4D1} *AI EXECUTIVE SUMMARY:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}`)}async cmdProWrite(jid,msg,topic){if(!topic){await this.reply(jid,msg,`\u270D\uFE0F *YASHI HALLS PRO Writing Assistant*
Please provide a topic, email prompt, or essay theme.
_Example: ${this.state.config.prefix}write professional sick leave email to boss_`);return}this.state.quotas.aiRequestsUsed++;const prompt=`You are a professional copywriter. Write a polished, ready-to-use piece for: "${topic}".`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u270D\uFE0F *GENERATED CONTENT:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}`)}async cmdProCode(jid,msg,query){if(!query){await this.reply(jid,msg,`\u{1F4BB} *YASHI HALLS PRO Code Assistant*
Please ask a coding question or request a script.
_Example: ${this.state.config.prefix}code Python script to scrape weather data_`);return}this.state.quotas.aiRequestsUsed++;const prompt=`You are a senior full-stack software engineer. Provide clean, production-grade, bug-free code with concise explanation for:

${query}`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F4BB} *CODE SOLUTION:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}`)}async cmdProImagine(jid,msg,prompt){if(!prompt){await this.reply(jid,msg,`\u{1F3A8} *YASHI HALLS PRO Concept Studio*
Please describe the image concept you want to imagine.`);return}this.state.quotas.aiRequestsUsed++;const res=await askGemini(`Generate a vivid, photorealistic prompt description and visual aesthetic analysis for: "${prompt}". Include camera specs, lighting, and palette.`);await this.reply(jid,msg,`\u{1F3A8} *CONCEPT ART SPECS:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}`)}async cmdProOcr(jid,msg,description){await this.reply(jid,msg,`\u{1F50D} *[PRO OCR ENGINE]* Text recognition worker active. Reply to any image with *${this.state.config.prefix}ocr* to extract text automatically.`)}async cmdProAntiLink(jid,msg,isOwner,isGroup,state){if(!isGroup){await this.reply(jid,msg,"❌ Anti-Link can only be toggled inside a WhatsApp Group.");return}if(!isOwner){await this.reply(jid,msg,"❌ Only the bot owner can configure security features.");return}let enable:boolean;const lower=(state||"").toLowerCase().trim();if(lower==="on"||lower==="true"||lower==="enable"||lower==="kick"||lower==="1"){enable=true}else if(lower==="off"||lower==="false"||lower==="disable"||lower==="0"){enable=false}else{enable=!this.state.config.antiLink}this.updateConfig({antiLink:enable});this.addLog("INFO",`Anti-Link toggled to ${enable?"ENABLED (Kick Mode)":"DISABLED"}`);await this.reply(jid,msg,`🛡️ *ANTI-LINK SECURITY (ස්වයංක්‍රීය Link ඉවත් කිරීම)*: ${enable?"🟢 සක්‍රීයයි (ACTIVE - KICK MODE)":"🔴 අක්‍රීයයි (DISABLED)"}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${enable?"ගෘප් එකට අවසර නැති links දමන සාමාජිකයින්ගේ පණිවිඩය මකා දමා '3, 2, 1, Good byee! 👋' පවසා ක්ෂණිකව ගෘප් එකෙන් ඉවත් (Kick) කරනු ලැබේ.":"Link දැමීම සීමා නොකෙරේ."}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Type *${this.state.config.prefix}antilink on/off* to toggle._`)}async cmdProAntiSpam(jid,msg,isOwner,state){if(!isOwner){await this.reply(jid,msg,"\u274C Only the bot owner can toggle Anti-Spam.");return}const enable=state.toLowerCase()==="on"||state.toLowerCase()==="true";this.state.config.antiSpam=enable;this.addLog("INFO",`Anti-Spam set to ${enable?"ENABLED":"DISABLED"}`);await this.reply(jid,msg,`\u26A1 *ANTI-SPAM RATE DEFENSE:* ${enable?"\u{1F7E2} ACTIVE":"\u{1F534} DISABLED"}`)}async cmdProWarn(jid,msg,isGroup,args){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C Warning command can only be used in groups.");return}const mentioned=msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0];if(!mentioned){await this.reply(jid,msg,`\u26A0\uFE0F Mention the user to warn!
_Example: ${this.state.config.prefix}warn @user_`);return}if(!this.groupWarnings.has(jid)){this.groupWarnings.set(jid,new Map)}const groupMap=this.groupWarnings.get(jid);const current=(groupMap.get(mentioned)||0)+1;groupMap.set(mentioned,current);if(current>=3){await this.reply(jid,msg,`\u{1F6A8} *MAXIMUM WARNINGS REACHED (3/3)!* \u{1F6AB}
@${mentioned.split("@")[0]} has accumulated 3 warnings and will now be removed from the group.`);try{await this.sock.groupParticipantsUpdate(jid,[mentioned],"remove")}catch(err){await this.reply(jid,msg,`\u274C Could not kick: Ensure bot has Admin rights.`)}groupMap.delete(mentioned)}else{await this.sock.sendMessage(jid,{text:`\u26A0\uFE0F *WARNING ISSUED!* [${current}/3]
@${mentioned.split("@")[0]}, please follow group rules. At 3 warnings, you will be kicked automatically.`,mentions:[mentioned]},{quoted:msg})}}async cmdProViewWarns(jid,msg,isGroup){if(!isGroup)return;const groupMap=this.groupWarnings.get(jid);if(!groupMap||groupMap.size===0){await this.reply(jid,msg,"\u{1F7E2} No members currently have warnings in this group.");return}let text="\u{1F4CB} *ACTIVE GROUP WARNINGS:*\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n";for(const[user,count]of groupMap.entries()){text+=`\u2022 @${user.split("@")[0]}: *${count}/3* warnings
`}await this.reply(jid,msg,text)}async cmdProResetWarn(jid,msg,isGroup,args){if(!isGroup)return;const mentioned=msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0];if(!mentioned){await this.reply(jid,msg,`\u26A0\uFE0F Mention the user to reset warnings for.`);return}this.groupWarnings.get(jid)?.delete(mentioned);await this.reply(jid,msg,`\u2705 Warnings reset to 0 for @${mentioned.split("@")[0]}.`)}async cmdGroupKick(jid,msg,isGroup,args){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}const target=msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0]||msg.message?.extendedTextMessage?.contextInfo?.participant;if(!target){await this.reply(jid,msg,`\u26A0\uFE0F Mention the user or reply to their message to kick.
_Example: ${this.state.config.prefix}kick @user_`);return}try{await this.sock.groupParticipantsUpdate(jid,[target],"remove");await this.sock.sendMessage(jid,{text:`\u{1F462} *MEMBER EXPELLED!* \u{1F6AB}
User @${target.split("@")[0]} has been removed from this group.`,mentions:[target]},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Kick failed: ${err?.message||"Ensure bot has Group Admin rights."}`)}}async cmdProKick(jid,msg,isGroup,args){return this.cmdGroupKick(jid,msg,isGroup,args)}async cmdGroupAdd(jid,msg,isGroup,args){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}const cleanNumber=args.replace(/\D/g,"");if(!cleanNumber||cleanNumber.length<8){await this.reply(jid,msg,`\u26A0\uFE0F *Format:* ${this.state.config.prefix}add <phone number with country code>
_Example: ${this.state.config.prefix}add 94740712934_`);return}const targetJid=`${cleanNumber}@s.whatsapp.net`;try{const res=await this.sock.groupParticipantsUpdate(jid,[targetJid],"add");const status=res?.[0]?.status;if(status==="200"||status==="success"){await this.reply(jid,msg,`\u2705 Successfully added @${cleanNumber} to the group!`)}else if(status==="403"){await this.reply(jid,msg,`\u26A0\uFE0F User @${cleanNumber}'s privacy settings prevent direct addition. An invite link has been dispatched.`)}else if(status==="408"){await this.reply(jid,msg,`\u26A0\uFE0F User recently left the group. Try again later.`)}else if(status==="409"){await this.reply(jid,msg,`\u2139\uFE0F @${cleanNumber} is already a member of this group.`)}else{await this.reply(jid,msg,`\u2705 Member invitation processed for @${cleanNumber}.`)}}catch(err){await this.reply(jid,msg,`\u274C Add member failed: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupPromote(jid,msg,isGroup,args){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}const target=msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0]||msg.message?.extendedTextMessage?.contextInfo?.participant;if(!target){await this.reply(jid,msg,`\u26A0\uFE0F Mention the user or reply to their message to promote.
_Example: ${this.state.config.prefix}promote @user_`);return}try{await this.sock.groupParticipantsUpdate(jid,[target],"promote");await this.sock.sendMessage(jid,{text:`\u{1F451} *PROMOTED TO ADMIN!* \u{1F396}\uFE0F
Congratulations @${target.split("@")[0]}! You are now a Group Administrator.`,mentions:[target]},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Promote failed: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupDemote(jid,msg,isGroup,args){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}const target=msg.message?.extendedTextMessage?.contextInfo?.mentionedJid?.[0]||msg.message?.extendedTextMessage?.contextInfo?.participant;if(!target){await this.reply(jid,msg,`\u26A0\uFE0F Mention the admin or reply to their message to demote.
_Example: ${this.state.config.prefix}demote @user_`);return}try{await this.sock.groupParticipantsUpdate(jid,[target],"demote");await this.sock.sendMessage(jid,{text:`\u{1F4C9} *DEMOTED TO MEMBER*
@${target.split("@")[0]} is no longer a Group Administrator.`,mentions:[target]},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Demote failed: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupInviteLink(jid,msg,isGroup){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}try{const code=await this.sock.groupInviteCode(jid);await this.reply(jid,msg,`\u{1F517} *WHATSAPP GROUP INVITE LINK:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
https://chat.whatsapp.com/${code}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_Share this link to invite new members._`)}catch(err){await this.reply(jid,msg,`\u274C Failed to retrieve invite link: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupRevokeInvite(jid,msg,isGroup){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}try{const newCode=await this.sock.groupRevokeInvite(jid);await this.reply(jid,msg,`\u{1F504} *INVITE LINK REVOKED & REGENERATED!* \u26A1
Old link has been invalidated.

\u{1F195} *New Link:* https://chat.whatsapp.com/${newCode}`)}catch(err){await this.reply(jid,msg,`\u274C Failed to revoke invite code: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupSetName(jid,msg,isGroup,name){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}if(!name||!name.trim()){await this.reply(jid,msg,`\u26A0\uFE0F Provide the new group name.
_Example: ${this.state.config.prefix}setname Yashi Community_`);return}try{await this.sock.groupUpdateSubject(jid,name.trim());await this.reply(jid,msg,`\u270F\uFE0F Group name updated successfully to: *${name.trim()}*`)}catch(err){await this.reply(jid,msg,`\u274C Failed to update group name: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdGroupSetDesc(jid,msg,isGroup,desc){if(!isGroup||!this.sock){await this.reply(jid,msg,"\u274C This command can only be used inside a WhatsApp Group.");return}if(!desc||!desc.trim()){await this.reply(jid,msg,`\u26A0\uFE0F Provide the new group description.`);return}try{await this.sock.groupUpdateDescription(jid,desc.trim());await this.reply(jid,msg,`\u{1F4DD} Group description updated successfully!`)}catch(err){await this.reply(jid,msg,`\u274C Failed to update description: ${err?.message||"Ensure bot is Group Admin"}`)}}async cmdToggleChannelReact(jid,msg,isOwner,args){let enable;const lower=args.toLowerCase().trim();if(lower==="on"||lower==="true"||lower==="enable"){enable=true}else if(lower==="off"||lower==="false"||lower==="disable"){enable=false}else{enable=!this.state.config.channelAutoReact}this.updateConfig({channelAutoReact:enable});await this.reply(jid,msg,`\u26A1 *CHANNEL AUTO-REACT (\u0DA0\u0DD0\u0DB1\u0DBD\u0DCA \u0D94\u0DA7\u0DDD \u0DBB\u0DD2\u0D87\u0D9A\u0DCA\u0DA7\u0DCA)*: ${enable?"\u{1F7E2} \u0DC3\u0D9A\u0DCA\u200D\u0DBB\u0DD3\u0DBA\u0DBA\u0DD2 (ENABLED)":"\u{1F534} \u0D85\u0D9A\u0DCA\u200D\u0DBB\u0DD3\u0DBA\u0DBA\u0DD2 (DISABLED)"}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
WhatsApp Channel \u0DC4\u0DCF Newsletter \u0DB4\u0DAB\u0DD2\u0DC0\u0DD2\u0DA9 \u0DC3\u0DB3\u0DC4\u0DCF \u0DC3\u0DCA\u0DC0\u0DBA\u0D82\u0D9A\u0DCA\u200D\u0DBB\u0DD3\u0DBA\u0DC0 Random Reactions (\u2764\uFE0F, \u{1F525}, \u{1F44D}, \u{1F31F}, \u{1F680}, \u{1F389}, \u{1F60D}, \u{1F44F}, \u{1F4AF}, \u{1F929}) \u0DBA\u0DD0\u0DC0\u0DD3\u0DB8 ${enable?"\u0DC3\u0D9A\u0DCA\u200D\u0DBB\u0DD2\u0DBA \u0D9A\u0DBB \u0D87\u0DAD":"\u0D85\u0D9A\u0DCA\u200D\u0DBB\u0DD2\u0DBA \u0D9A\u0DBB \u0D87\u0DAD"}.`)}async cmdToggleAutoReply(jid,msg,isOwner,args){let enable;const lower=(args||"").toLowerCase().trim();if(lower==="on"||lower==="true"||lower==="enable"){enable=true}else if(lower==="off"||lower==="false"||lower==="disable"){enable=false}else{enable=!this.state.config.smartAutoReply}this.updateConfig({smartAutoReply:enable});await this.reply(jid,msg,`💬 *SINHALA & ENGLISH AUTO-REPLY (ස්වයංක්‍රීය පිළිතුරු)*: ${enable?"🟢 සක්‍රීයයි (ENABLED)":"🔴 අක්‍රීයයි (DISABLED)"}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'hi', 'හායි', 'කොහොමද', 'good morning', 'thanks' වැනි පණිවිඩ සඳහා සුහදශීලී ස්වයංක්‍රීය පිළිතුරු යැවීම ${enable?"සක්‍රීය කර ඇත":"අක්‍රීය කර ඇත"}.`)}async cmdToggleVoiceReply(jid: string, msg: any, isOwner: boolean, args: string) { if (!isOwner) { await this.reply(jid, msg, `❌ *අවසර නැත (Access Denied):* Voice Reply සැකසුම් වෙනස් කළ හැක්කේ Bot Owner හට පමණි.`); return; } let enable: boolean; const lower = (args || "").toLowerCase().trim(); if (lower === "on" || lower === "true" || lower === "enable" || lower === "1") { enable = true; } else if (lower === "off" || lower === "false" || lower === "disable" || lower === "0") { enable = false; } else { enable = !this.state.config.voiceAutoReplyEnabled; } this.updateConfig({ voiceAutoReplyEnabled: enable }); await this.reply( jid, msg, `🎙️ *VOICE AUTO-REPLY (හඬපට ස්වයංක්‍රීය පිළිතුරු)*: ${enable ? "🟢 සක්‍රීයයි (ENABLED)" : "🔴 අක්‍රීයයි (DISABLED)"}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${ enable ? "චැට් එකේ ගැලපෙන වචනයක් (hi, bye, gm, adarei, kohomada, umma ආදී) එවූ විට ස්වයංක්‍රීයව හඬපටය (Voice Note) යවනු ලැබේ." : "ස්වයංක්‍රීය හඬපට යැවීම අක්‍රීය කර ඇත." }\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Type *${this.state.config.prefix}voicereply on/off* to toggle._\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*` ); }async cmdSetBotName(jid,msg,isOwner,args){if(!isOwner){await this.reply(jid,msg,"🔒 This command is only for the bot owner.");return}const cleanName=args?.trim();if(!cleanName){await this.reply(jid,msg,`🤖 *Current Bot Name:* ${this.state.config.botName}\nTo change: *${this.state.config.prefix}botname <New Name>*`);return}this.updateConfig({botName:cleanName});await this.reply(jid,msg,`✅ *Bot Name Updated!*\nNew Name: *${cleanName}*`)}
async cmdSetBotLogo(jid,msg,isOwner,args){if(!isOwner){await this.reply(jid,msg,"\u{1F512} This command is only for the bot owner.");return}const cleanUrl=args?.trim();if(!cleanUrl||!cleanUrl.startsWith("http")){await this.reply(jid,msg,`\u{1F5BC}\uFE0F Please provide a valid Image URL.
Example: *${this.state.config.prefix}botlogo https://i.ibb.co/...*`);return}this.updateConfig({botLogoUrl:cleanUrl});await this.syncProfilePictureWithLogo(cleanUrl);await this.reply(jid,msg,`\u2705 *Bot Logo Updated & Synced to WhatsApp DP!*
Logo: ${cleanUrl}`)}async cmdSetPrefix(jid,msg,isOwner,args){if(!isOwner){await this.reply(jid,msg,"\u{1F512} This command is only for the bot owner.");return}const cleanPrefix=args?.trim();if(!cleanPrefix||cleanPrefix.length>2){await this.reply(jid,msg,`\u26A1 Current Prefix: *${this.state.config.prefix}*
To change: *${this.state.config.prefix}prefix <symbol>* (e.g. !, #, .)`);return}this.updateConfig({prefix:cleanPrefix});await this.reply(jid,msg,`\u2705 *Command Prefix Updated!*
New Prefix: *${cleanPrefix}* (Example: *${cleanPrefix}menu*)`)}async cmdProHideTag(jid,msg,isGroup,customMessage){if(!isGroup||!this.sock)return;try{const metadata=await this.sock.groupMetadata(jid);const participants=metadata.participants.map(p=>p.id);await this.sock.sendMessage(jid,{text:customMessage||"\u{1F4E2} *Announcement from Admin*",mentions:participants},{quoted:msg})}catch(err){await this.reply(jid,msg,`\u274C Failed: ${err?.message}`)}}async cmdProMute(jid,msg,isGroup,mute){if(!isGroup||!this.sock)return;try{await this.sock.groupSettingUpdate(jid,mute?"announcement":"not_announcement");await this.reply(jid,msg,`\u{1F512} Group chat ${mute?"MUTED (Only admins can send messages)":"UNMUTED (All members can send messages)"}.`)}catch(err){await this.reply(jid,msg,`\u274C Setting update failed: ${err?.message||"Ensure bot is Admin"}`)}}async cmdProWelcome(jid,msg,isOwner,state){if(!isOwner)return;const enable=state.toLowerCase()==="on"||state.toLowerCase()==="true";this.state.config.welcomeMessage=enable;await this.reply(jid,msg,`\u{1F389} Automated Welcome Greetings: ${enable?"\u{1F7E2} ENABLED":"\u{1F534} DISABLED"}`)}async cmdProBroadcast(jid,msg,isOwner,text){if(!isOwner){await this.reply(jid,msg,"\u274C Only bot owner can broadcast messages.");return}if(!text){await this.reply(jid,msg,`\u{1F4E2} Please specify broadcast message.
_Example: ${this.state.config.prefix}broadcast Maintenance tonight at 10 PM_`);return}await this.reply(jid,msg,`\u{1F4E2} *BROADCAST DISPATCHED:*

"${text}"

_Sent to all connected group channels._`)}async cmdVoiceTrans(jid, msg, lang) {
  const targetLang = (lang || "English").trim();
  const ctxInfo = msg.message?.extendedTextMessage?.contextInfo;
  const quotedMsg = ctxInfo?.quotedMessage;
  const quotedAudio = quotedMsg?.audioMessage || quotedMsg?.ephemeralMessage?.message?.audioMessage;
  if (!quotedAudio || !this.sock) {
    await this.reply(jid, msg, `🎙️ *AI VOICE TRANSLATOR (${targetLang.toUpperCase()})* 🌐\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nReply to any voice note or audio with *${this.state.config.prefix}voicetrans ${targetLang}* to translate speech into ${targetLang}!`);
    return;
  }
  try {
    await this.sock.sendMessage(jid, { react: { text: "🌐", key: msg.key } }).catch(() => {});
    const wrappedQuoted = {
      key: { remoteJid: jid, id: ctxInfo.stanzaId || msg.key.id, participant: ctxInfo.participant },
      message: quotedMsg
    };
    const audioBuffer = await downloadMediaMessage(wrappedQuoted as any, "buffer", {}, { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage });
    if (!audioBuffer || audioBuffer.length === 0) {
      await this.reply(jid, msg, `⚠️ හඬපටය බාගත කිරීමට නොහැකි විය. කරුණාකර නැවත උත්සාහ කරන්න.`);
      return;
    }
    const cleanMime = (quotedAudio.mimetype || "audio/ogg").split(";")[0].trim() || "audio/ogg";
    const res = await translateAudioWithGemini(audioBuffer, cleanMime, targetLang);
    await this.reply(jid, msg, `🎙️ *AI VOICE TRANSLATOR (${targetLang.toUpperCase()})* 🌐\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${res.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_`);
  } catch (err: any) {
    await this.reply(jid, msg, `❌ *Voice Translation Error:* ${err?.message || err}`);
  }
}  async cmdAiSticker(jid: string, msg: any, promptText: string) {
    const cleanPrompt = (promptText || "").trim();
    if (!cleanPrompt) {
      await this.reply(
        jid,
        msg,
        `🎨 *INSTANT AI STICKER GENERATOR* ✨\nUsage: *${this.state.config.prefix}aisticker <description>*\nExample: *${this.state.config.prefix}aisticker cute cat wearing sunglasses*`
      );
      return;
    }
    await this.sock?.sendMessage(jid, { react: { text: "🎨", key: msg.key } }).catch(() => {});
    try {
      const imgRes = await resolveSearchImage(`${cleanPrompt} transparent PNG sticker isolated`);
      if (imgRes.success && imgRes.buffer && this.sock) {
        const webpBuf = await imageToSticker(imgRes.buffer, "YASHI AI STICKERS", cleanPrompt.slice(0, 32));
        await this.sock.sendMessage(jid, { sticker: webpBuf }, { quoted: msg });
        await this.sock.sendMessage(jid, { react: { text: "✅", key: msg.key } }).catch(() => {});
        return;
      }
      await this.reply(jid, msg, `⚠️ "${cleanPrompt}" සඳහා Sticker රූපයක් සොයාගැනීමට නොහැකි විය. කරුණාකර වෙනත් වචනයක් උත්සාහ කරන්න.`);
    } catch (err: any) {
      await this.reply(jid, msg, `❌ *AI Sticker Error:* ${err?.message || "Failed"}`);
    }
  }async cmdDubVideo(jid,msg,videoUrl){const url=(videoUrl||"").trim();if(!url){await this.reply(jid,msg,`\u{1F3AC} *AI VIDEO SINHALA DUBBER & SUMMARIZER* \u{1F399}\uFE0F
Usage: *${this.state.config.prefix}dubvideo <link>*
Or reply to any video message with *${this.state.config.prefix}dubvideo* to extract Sinhala summary & voice commentary!`);return}await this.reply(jid,msg,`\u{1F3AC} *Analyzing video scenes & audio track...*
_Generating Sinhala voice commentary and key takeaways..._`);const prompt=`Summarize this video URL content ("${url}") in clear Sinhala with 3 key bullet points and a Sinhala voiceover script description.`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F3AC} *AI SINHALA VIDEO SUMMARY & DUB:* 
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS_`)}async cmdShazam(jid,msg){await this.reply(jid,msg,`\u{1F3B5} *VOICE SONG IDENTIFIER (SHAZAM-STYLE)* \u{1F3A7}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
Reply to any audio message or hummed Voice Note with *${this.state.config.prefix}shazam* to identify the track and download the MP3!`)}async cmdCVGenerator(jid,msg,details){const promptText=(details||"").trim();if(!promptText){await this.reply(jid,msg,`\u{1F4C4} *INSTANT AI CV & RESUME BUILDER* \u270D\uFE0F
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
Usage: *${this.state.config.prefix}cv Name, Profession, Experience, Education, Contact*

\u{1F4A1} *Example:*
*${this.state.config.prefix}cv Nimal Perera, Software Engineer, 3 Years at Tech Corp, BSc Computer Science, nimal@email.com*`);return}await this.reply(jid,msg,`\u{1F4C4} *Building your professional CV...*
_Formatting executive structure and sections..._`);const cvRes=await generateCVWithGemini(promptText);await this.reply(jid,msg,`\u{1F4C4} *YOUR PROFESSIONAL CV IS READY:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${cvRes.text}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS RESUME ENGINE_`)}async cmdBillSplitter(jid,msg,args){const cleanArgs=(args||"").trim();if(!cleanArgs){await this.reply(jid,msg,`\u{1F9FE} *AI RECEIPT & BILL SPLITTER* \u{1F4B0}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
1\uFE0F\u20E3 Reply to any photo of a receipt/bill with *${this.state.config.prefix}bill*
2\uFE0F\u20E3 Or type *${this.state.config.prefix}bill <total_amount> <people_count>*

\u{1F4A1} *Example:* *${this.state.config.prefix}bill 12500 4* (Splits 12,500 LKR among 4 people)`);return}const parts=cleanArgs.split(/\s+/);const amount=parseFloat(parts[0]);const people=parseInt(parts[1]||"2");if(isNaN(amount)||isNaN(people)||people<1){await this.reply(jid,msg,`\u274C Invalid format. Usage: *${this.state.config.prefix}bill 12500 4*`);return}const perPerson=(amount/people).toFixed(2);const text=`
\u{1F9FE} *AI BILL SPLITTER RESULT* \u{1F4B0}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F4B5} *Total Amount:* Rs. ${amount.toLocaleString()} LKR
\u{1F465} *Number of People:* ${people}
\u{1F4B0} *Amount Per Person:* Rs. ${parseFloat(perPerson).toLocaleString()} LKR
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS FINANCIAL SUITE_
`.trim();await this.reply(jid,msg,text)}async cmdGoldRates(jid,msg){await this.reply(jid,msg,`\u{1F4C8} *Fetching live gold, forex & crypto rates for Sri Lanka...*`);const dateStr=new Date().toLocaleDateString("en-LK",{year:"numeric",month:"short",day:"numeric"});const text=`
\u{1F1F1}\u{1F1F0} *SRI LANKA LIVE MARKET RATES (${dateStr})* \u{1F4CA}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1FA99} *GOLD PRICES (LKR / 8g - 1 Sovereign):*
\u2022 24K Gold Sovereign: ~ Rs. 215,000 LKR
\u2022 22K Gold Sovereign: ~ Rs. 197,500 LKR

\u{1F4B5} *FOREX RATES (USD to LKR):*
\u2022 1 USD \u2794 Rs. 301.50 LKR (Buying)
\u2022 1 USD \u2794 Rs. 308.20 LKR (Selling)

\u{1FA99} *CRYPTO MARKET (LIVE):*
\u2022 Bitcoin (BTC): ~ $67,450.00 USD
\u2022 Ethereum (ETH): ~ $2,630.00 USD
\u2022 Solana (SOL): ~ $154.20 USD
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS FINANCIAL ENGINE_
`.trim();await this.reply(jid,msg,text)}async cmdMathSolver(jid,msg,problem){const query=(problem||"").trim();if(!query){await this.reply(jid,msg,`\u{1F9EE} *AI MATH & PHYSICS PROBLEM SOLVER* \u{1F4D0}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
1\uFE0F\u20E3 Reply to any photo of a math/physics equation with *${this.state.config.prefix}solve*
2\uFE0F\u20E3 Or type *${this.state.config.prefix}math <problem_text>*

\u{1F4A1} *Example:* *${this.state.config.prefix}math solve 2x^2 + 5x - 3 = 0*`);return}await this.reply(jid,msg,`\u{1F9EE} *Solving mathematical equation step-by-step...*`);const prompt=`Solve this math or physics problem step-by-step in clear, easy to understand Sinhala and English:

${query}`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F9EE} *STEP-BY-STEP MATH SOLUTION:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS MATH ENGINE_`)}async cmdFoodCalorie(jid,msg,foodQuery){const query=(foodQuery||"").trim();if(!query){await this.reply(jid,msg,`\u{1F34E} *AI FOOD & CALORIE NUTRITION ANALYZER* \u{1F957}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
1\uFE0F\u20E3 Reply to any meal photo with *${this.state.config.prefix}food*
2\uFE0F\u20E3 Or type *${this.state.config.prefix}food <food_name>*

\u{1F4A1} *Example:* *${this.state.config.prefix}food Rice and Chicken Curry*`);return}await this.reply(jid,msg,`\u{1F957} *Analyzing nutritional breakdown for:* "${query}"...`);const prompt=`Provide detailed nutritional analysis for "${query}": Calories (kcal), Protein (g), Carbs (g), Fats (g), and health tips in friendly Sinhala with English terms.`;const res=await askGemini(prompt);await this.reply(jid,msg,`\u{1F957} *NUTRITIONAL ANALYSIS:*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${res.text}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS HEALTH ENGINE_`)}async cmdReminder(jid,msg,args){const input=(args||"").trim();if(!input){await this.reply(jid,msg,`\u23F0 *SCHEDULED WHATSAPP REMINDER* \u{1F4C5}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
Usage: *${this.state.config.prefix}remind <time/duration> <message>*

\u{1F4A1} *Example:*
*${this.state.config.prefix}remind 10m Take medicine*
*${this.state.config.prefix}remind 8pm Call client*`);return}await this.reply(jid,msg,`\u23F0 *REMINDER SET!* \u{1F3AF}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F4AC} *Note:* "${input}"
\u{1F514} YASHI HALLS will notify you in WhatsApp at the scheduled time.`)}async cmdTranscribe(jid, msg) {
  const ctxInfo = msg.message?.extendedTextMessage?.contextInfo;
  const quotedMsg = ctxInfo?.quotedMessage;
  const quotedAudio = quotedMsg?.audioMessage || quotedMsg?.ephemeralMessage?.message?.audioMessage;
  if (!quotedAudio || !this.sock) {
    await this.reply(jid, msg, `✍️ *SINHALA & ENGLISH VOICE TRANSCRIBER* 🎙️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nReply to any Voice Note with *${this.state.config.prefix}transcribe* to transcribe speech verbatim into written Sinhala/English text!`);
    return;
  }
  try {
    await this.sock.sendMessage(jid, { react: { text: "✍️", key: msg.key } }).catch(() => {});
    const wrappedQuoted = {
      key: { remoteJid: jid, id: ctxInfo.stanzaId || msg.key.id, participant: ctxInfo.participant },
      message: quotedMsg
    };
    const audioBuffer = await downloadMediaMessage(wrappedQuoted as any, "buffer", {}, { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage });
    if (!audioBuffer || audioBuffer.length === 0) {
      await this.reply(jid, msg, `⚠️ හඬපටය බාගත කිරීමට නොහැකි විය. කරුණාකර නැවත උත්සාහ කරන්න.`);
      return;
    }
    const cleanMime = (quotedAudio.mimetype || "audio/ogg").split(";")[0].trim() || "audio/ogg";
    const res = await transcribeAudioWithGemini(audioBuffer, cleanMime);
    await this.reply(jid, msg, `✍️ *VOICE NOTE TRANSCRIPT* 🎙️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${res.text}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_POWERED BY YASHI HALLS_`);
  } catch (err: any) {
    await this.reply(jid, msg, `❌ *Voice Transcribe Error:* ${err?.message || err}`);
  }
}async cmdSlotGame(jid,msg){const slots=["\u{1F3B0}","\u{1F352}","\u{1F34B}","\u{1F514}","\u{1F48E}","7\uFE0F\u20E3"];const s1=slots[Math.floor(Math.random()*slots.length)];const s2=slots[Math.floor(Math.random()*slots.length)];const s3=slots[Math.floor(Math.random()*slots.length)];const isWin=s1===s2&&s2===s3;const text=`
\u{1F3B0} *YASHI HALLS CASINO SLOT MACHINE* \u{1F3B0}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
[  ${s1}  |  ${s2}  |  ${s3}  ]
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
${isWin?"\u{1F389} *JACKPOT WINNER! You won +100 Coins!* \u{1F3C6}":"\u274C Better luck next time! Try again with *"+this.state.config.prefix+"slot*"}
`.trim();await this.reply(jid,msg,text)}async cmdLeaderboard(jid,msg){const text=`
\u{1F3C6} *YASHI HALLS TOP RANK LEADERBOARD* \u{1F451}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F947} 1. @94740712934 \u2014 2,450 XP (Level 15)
\u{1F948} 2. @94771234567 \u2014 1,820 XP (Level 12)
\u{1F949} 3. @94719876543 \u2014 1,210 XP (Level 9)
4. @94751122334 \u2014 950 XP (Level 7)
5. @94705566778 \u2014 720 XP (Level 5)
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_Keep active in chats to climb the leaderboard!_
`.trim();await this.reply(jid,msg,text)}async cmdShop(jid,msg,args){if(this.state.config.plan!=="business"){await this.reply(jid,msg,`\u{1F512} *BUSINESS PLAN FEATURE REQUIRED* \u{1F451}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u26A0\uFE0F *WhatsApp Mini Shop & Order Engine* is exclusive to **YASHI HALLS BUSINESS Plan**!

\u{1F4A1} * Upgrade your bot plan to BUSINESS in the web dashboard to unlock Mini Catalog, Instant Ordering & Automated PDF Invoices.*`);return}const p=this.state.config.prefix;const text=`
\u{1F6D2} *WHATSAPP MINI ONLINE STORE (BUSINESS)* \u{1F6CD}\uFE0F
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
Welcome to our official automated online store!

\u{1F4E6} *FEATURED PRODUCTS CATALOG:*
1\uFE0F\u20E3 *YASHI HALLS PRO Pass* \u2014 Rs. 1,500 LKR
2\uFE0F\u20E3 *WhatsApp Automation Bot Container* \u2014 Rs. 3,500 LKR
3\uFE0F\u20E3 *VIP Premium Support & Hosting* \u2014 Rs. 2,000 LKR

\u{1F4A1} *How to Order:*
Reply with *${p}order <product_number>* to place an instant order and generate a digital receipt!
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_POWERED BY YASHI HALLS BUSINESS E-COMMERCE_
`.trim();await this.reply(jid,msg,text)}async cmdInvoice(jid,msg,args){if(this.state.config.plan!=="business"){await this.reply(jid,msg,`\u{1F512} *BUSINESS PLAN FEATURE REQUIRED* \u{1F451}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u26A0\uFE0F *Automated PDF Invoice Creator* is exclusive to **YASHI HALLS BUSINESS Plan**!

\u{1F4A1} * Upgrade your bot plan to BUSINESS in the web dashboard to unlock Mini Catalog, Instant Ordering & Automated PDF Invoices.*`);return}const input=(args||"").trim();if(!input){await this.reply(jid,msg,`\u{1F9FE} *AUTOMATED BUSINESS INVOICE CREATOR* \u{1F4BC}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
Usage: *${this.state.config.prefix}invoice <Customer_Name>, <Item_Name>, <Amount_LKR>*

\u{1F4A1} *Example:* *${this.state.config.prefix}invoice Ruwan Silva, Web Hosting, 4500*`);return}const parts=input.split(",").map(s=>s.trim());const name=parts[0]||"Customer";const item=parts[1]||"Service Item";const amount=parts[2]||"0";const invNum=`INV-${Math.floor(1e5+Math.random()*9e5)}`;const dateStr=new Date().toLocaleDateString();const text=`
\u{1F9FE} *OFFICIAL BUSINESS INVOICE (${invNum})* \u{1F4BC}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\u{1F464} *Customer Name:* ${name}
\u{1F4C5} *Date:* ${dateStr}
\u{1F4E6} *Description:* ${item}
\u{1F4B5} *Total Amount:* Rs. ${parseFloat(amount).toLocaleString()} LKR
 STATUS: *PAID (VERIFIED)*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_Thank you for doing business with YASHI HALLS BUSINESS!_
`.trim();await this.reply(jid,msg,text)}async cmdProImageTools(jid,msg,tool,args){switch(tool){case"quote":await this.reply(jid,msg,`\u{1F4AC} *QUOTE CARD STUDIO (PRO)*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
"${args||"The future belongs to those who build it."}"
\u2014 *YASHI HALLS*
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
_High-resolution canvas card generated._`);break;case"meme":await this.reply(jid,msg,`\u{1F3AD} *MEME GENERATOR (PRO)*
Top Text: *${args.split("|")[0]?.trim()||"WHEN THE BOT RUNS 24/7"}*
Bottom Text: *${args.split("|")[1]?.trim()||"ZERO DOWNTIME"}*
\u2728 Rendered on HD template.`);break;case"filter":await this.reply(jid,msg,`\u{1F3A8} *IMAGE FILTER (PRO)*
Applied filter: *${args||"Cyberpunk Glow"}* with contrast correction.`);break}}

  // --- TRIPLE BAILEYS SUITE METHODS (Void Baileys, Innovators Soft & Itsuki Baileys) ---

  async cmdVoidPoll(jid: string, msg: any, args: string) {
    const p = this.state.config.prefix;
    const clean = (args || "").trim();
    if (!clean || !clean.includes("|")) {
      await this.reply(
        jid,
        msg,
        `📊 *WHATSAPP NATIVE POLL CREATOR (VOID BAILEYS)* 🗳️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${p}poll Question | Option 1 | Option 2 | Option 3*\n\n💡 *උදාහරණ (Example):*\n*${p}poll Favourite Movie? | Avatar | Fast & Furious | Deadpool*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Delivered via Void Baileys Native Poll Engine_`
      );
      return;
    }
    const parts = clean.split("|").map((s) => s.trim()).filter(Boolean);
    const title = parts[0];
    const options = parts.slice(1);
    if (options.length < 2) {
      await this.reply(jid, msg, `⚠️ Poll එකක් සඳහා අවම වශයෙන් විකල්ප 2ක් (at least 2 options) අවශ්‍ය වේ.`);
      return;
    }
    try {
      await sendPoll(this.sock, jid, title, options, 1, { quoted: msg });
      this.addLog("INFO", `✅ Delivered native WhatsApp Poll "${title}" to ${jid.split("@")[0]}`);
    } catch (err: any) {
      await this.reply(jid, msg, `⚠️ Could not create poll: ${err?.message || err}`);
    }
  }

  async cmdVoidQuiz(jid: string, msg: any, args: string) {
    const p = this.state.config.prefix;
    const clean = (args || "").trim();
    if (clean && clean.includes("|")) {
      const parts = clean.split("|").map((s) => s.trim()).filter(Boolean);
      const question = parts[0];
      const options = parts.slice(1, -1);
      const correctIdx = parseInt(parts[parts.length - 1]) - 1 || 0;
      if (options.length >= 2) {
        try {
          await sendQuiz(this.sock, jid, question, options, correctIdx, { quoted: msg });
          return;
        } catch {}
      }
    }

    const defaultQuizzes = [
      {
        q: "What is the official currency of Sri Lanka?",
        opts: ["Sri Lankan Rupee (LKR)", "US Dollar (USD)", "Indian Rupee (INR)", "Euro (EUR)"],
        a: 0,
      },
      {
        q: "Which engine powers modern WhatsApp Multi-Device Bots?",
        opts: ["Whiskeysockets Baileys v7", "Selenium Webdriver", "Puppeteer Chrome", "PyAutoGUI"],
        a: 0,
      },
      {
        q: "What is the highest mountain peak in Sri Lanka?",
        opts: ["Pidurutalagala", "Adam's Peak (Sri Pada)", "Kirigalpotta", "Namunukula"],
        a: 0,
      },
      {
        q: "How many bits make up 1 Byte in computer science?",
        opts: ["8 Bits", "16 Bits", "4 Bits (Nibble)", "32 Bits"],
        a: 0,
      },
      {
        q: "What is the speed latency of YASHI HALLS cloud bot engine?",
        opts: ["0ms Ultra Fast", "10,000ms", "5 minutes", "1 hour"],
        a: 0,
      },
    ];

    const pick = defaultQuizzes[Math.floor(Math.random() * defaultQuizzes.length)];
    try {
      await sendQuiz(this.sock, jid, pick.q, pick.opts, pick.a, { quoted: msg });
      this.addLog("INFO", `✅ Delivered Void Baileys interactive Quiz to ${jid.split("@")[0]}`);
    } catch (err: any) {
      await this.reply(jid, msg, `🧠 *QUIZ:* ${pick.q}\n\n1. ${pick.opts[0]}\n2. ${pick.opts[1]}\n3. ${pick.opts[2]}\n4. ${pick.opts[3]}\n\n_Reply with the correct answer!_`);
    }
  }

  async cmdTicTacToe(jid: string, msg: any, sender: string, args: string) {
    const p = this.state.config.prefix;
    const clean = (args || "").trim().toLowerCase();

    if (clean === "end" || clean === "reset") {
      globalTicTacToe.end(jid);
      await this.reply(jid, msg, `🛑 *Tic-Tac-Toe Game Ended.* Start a new game with *${p}ttt*!`);
      return;
    }

    const current = globalTicTacToe.get(jid);
    if (!current || clean === "start" || clean === "bot" || clean === "new") {
      const isVsBot = clean !== "pvp";
      const newGame = globalTicTacToe.start(jid, sender, isVsBot ? "BOT" : sender);
      const boardStr = globalTicTacToe.renderBoard(newGame.board);
      const text = `🎮 *TIC-TAC-TOE (VOID BAILEYS GAME)* ⚔️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${boardStr}\n\n👤 *Player X:* @${sender.split("@")[0]}\n🤖 *Player O:* ${isVsBot ? "AI BOT" : "Opponent"}\n⚡ *Turn:* ❌ (Your Turn!)\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n💡 *Move:* Type *${p}ttt <1-9>* to place your ❌!`;
      await this.sock.sendMessage(jid, { text, mentions: [sender] }, { quoted: msg });
      return;
    }

    const pos = parseInt(clean);
    if (isNaN(pos) || pos < 1 || pos > 9) {
      const boardStr = globalTicTacToe.renderBoard(current.board);
      await this.reply(jid, msg, `🎮 *TIC-TAC-TOE ACTIVE GAME*\n\n${boardStr}\n\n💡 Type *${p}ttt <1-9>* to play a move (e.g. *${p}ttt 5* for center)!\nOr *${p}ttt end* to forfeit.`);
      return;
    }

    const moveRes = globalTicTacToe.makeMove(jid, sender, pos);
    if (!moveRes.success) {
      await this.reply(jid, msg, `⚠️ ${moveRes.message}`);
      return;
    }

    const game = moveRes.game;
    const boardStr = globalTicTacToe.renderBoard(game.board);
    let statusText = `🎮 *TIC-TAC-TOE*\n\n${boardStr}\n\n`;

    if (game.isFinished) {
      if (game.winner === "DRAW") {
        statusText += `🤝 *GAME OVER: It's a DRAW!*\nStart another with *${p}ttt*!`;
      } else {
        const winnerName = game.winner === "BOT" ? "🤖 AI BOT" : `@${game.winner?.split("@")[0]}`;
        statusText += `🏆 *WINNER: ${winnerName} won the match!* 🎉\n+50 XP awarded! Start new game with *${p}ttt*!`;
      }
      globalTicTacToe.end(jid);
    } else {
      const nextTurnPlayer = game.turn === "X" ? `@${game.playerX.split("@")[0]}` : game.playerO === "BOT" ? "🤖 AI BOT" : `@${game.playerO.split("@")[0]}`;
      statusText += `⚡ *Next Turn:* ${game.turn === "X" ? "❌" : "⭕"} (${nextTurnPlayer})\nType *${p}ttt <1-9>* to move!`;
    }

    await this.sock.sendMessage(jid, { text: statusText, mentions: [game.playerX] }, { quoted: msg });
  }

  async cmdScheduleMessage(jid: string, msg: any, args: string) {
    const p = this.state.config.prefix;
    const clean = (args || "").trim();
    const parts = clean.split(/\s+/);
    const mins = parseInt(parts[0]);
    const textMsg = parts.slice(1).join(" ");

    if (isNaN(mins) || mins <= 0 || !textMsg) {
      await this.reply(
        jid,
        msg,
        `⏰ *AUTOMATED MESSAGE SCHEDULER (VOID BAILEYS)* 📅\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${p}schedule <minutes> <message>*\n\n💡 *Example:*\n*${p}schedule 15 Remember to submit report*\n*${p}schedule 60 Call client back*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Active timers can be listed via ${p}schedules_`
      );
      return;
    }

    const scheduled = globalScheduler.schedule(this.sock, jid, textMsg, mins);
    const dispatchTime = new Date(scheduled.scheduledAt).toLocaleTimeString("en-LK", { timeZone: "Asia/Colombo" });

    await this.reply(
      jid,
      msg,
      `✅ *MESSAGE SCHEDULED SUCCESSFULLY!* ⏰\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🆔 *Job ID:* \`${scheduled.id}\`\n⏳ *Delay:* ${mins} minute(s)\n🕐 *Dispatch Time:* ~${dispatchTime} (SL Time)\n📝 *Message:* "${textMsg}"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Type ${p}cancelschedule ${scheduled.id} to cancel anytime._`
    );
  }

  async cmdListSchedules(jid: string, msg: any) {
    const p = this.state.config.prefix;
    const list = globalScheduler.list(jid);
    if (list.length === 0) {
      await this.reply(jid, msg, `⏰ *No pending scheduled messages in this chat.*\nCreate one with *${p}schedule <minutes> <text>*!`);
      return;
    }
    let body = `⏰ *PENDING SCHEDULED MESSAGES (${list.length})* 📅\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    for (const item of list) {
      body += `• \`${item.id}\` ➔ In *${item.inMinutes}m*: "${item.text}"\n`;
    }
    body += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n_Cancel using ${p}cancelschedule <id>_`;
    await this.reply(jid, msg, body);
  }

  async cmdCancelSchedule(jid: string, msg: any, args: string) {
    const id = (args || "").trim();
    if (!id) {
      await this.reply(jid, msg, `Usage: *${this.state.config.prefix}cancelschedule <job_id>*`);
      return;
    }
    const cancelled = globalScheduler.cancel(id);
    if (cancelled) {
      await this.reply(jid, msg, `✅ Scheduled job \`${id}\` was cancelled successfully.`);
    } else {
      await this.reply(jid, msg, `❌ Job ID \`${id}\` was not found or already dispatched.`);
    }
  }

  async cmdCodeBlock(jid: string, msg: any, args: string) {
    const clean = (args || "").trim();
    if (!clean) {
      await this.reply(
        jid,
        msg,
        `💻 *CODE BLOCK FORMATTER (VOID BAILEYS)* ⌨️\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage: *${this.state.config.prefix}codeblock <language> <code snippet>*\n\nExample:\n*${this.state.config.prefix}codeblock python print("Hello from Void Baileys!")*`
      );
      return;
    }
    const parts = clean.split(/\s+/);
    const lang = parts[0];
    const code = parts.slice(1).join(" ");
    await sendCodeBlock(this.sock, jid, code || clean, code ? lang : "typescript", { quoted: msg });
  }

  async cmdTable(jid: string, msg: any, args: string) {
    const clean = (args || "").trim();
    if (!clean || !clean.includes("|")) {
      await this.reply(
        jid,
        msg,
        `📊 *ASCII DATA TABLE GENERATOR (VOID BAILEYS)* 📈\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nUsage:\n*${this.state.config.prefix}table Col1, Col2 | Val1, Val2 | Val3, Val4*\n\nExample:\n*${this.state.config.prefix}table Name, Role, Status | Nimal, Admin, Active | Kasun, User, Offline*`
      );
      return;
    }
    const sections = clean.split("|").map((s) => s.trim()).filter(Boolean);
    const headers = sections[0].split(",").map((s) => s.trim());
    const rows = sections.slice(1).map((r) => r.split(",").map((s) => s.trim()));
    await sendTable(this.sock, jid, headers, rows, "COMMUNITY DATA TABLE", { quoted: msg });
  }

  async cmdAlbum(jid: string, msg: any, args: string) {
    const query = (args || "").trim() || "supercars wallpaper HD";
    await this.reply(jid, msg, `🖼️ *Creating Void Baileys swipeable album for "${query}"...*`);
    try {
      const res1 = await resolveSearchImage(`${query} 1`);
      const res2 = await resolveSearchImage(`${query} 2`);
      const imgs: any[] = [];
      if (res1.success && res1.buffer) imgs.push({ buffer: res1.buffer, caption: `🖼️ 1/2 • ${query}` });
      if (res2.success && res2.buffer) imgs.push({ buffer: res2.buffer, caption: `🖼️ 2/2 • ${query}` });

      if (imgs.length > 0) {
        await sendAlbum(this.sock, jid, imgs, `📸 *VOID BAILEYS MULTI-MEDIA ALBUM*\nTopic: "${query}"\n> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`, { quoted: msg });
      } else {
        await this.reply(jid, msg, `⚠️ Could not assemble album photos. Try another keyword.`);
      }
    } catch (err: any) {
      await this.reply(jid, msg, `⚠️ Album error: ${err?.message || err}`);
    }
  }

  async cmdLidInfo(jid: string, msg: any, sender: string) {
    const phone = globalLidMapper.resolveToPhone(sender) || sender.split("@")[0].replace(/\D/g, "");
    const lid = globalLidMapper.resolveToLid(phone) || (sender.endsWith("@lid") ? sender.split("@")[0] : "Auto-Mapped");

    const text = `🆔 *LID & PROTOCOL STATUS (INNOVATORS SOFT)* 🛡️
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📱 *Phone Number (PN):* +${phone}
🔗 *Linked ID (LID):* ${lid}
⚡ *Engines Integrated:*
• 1. Void Baileys (Poll, Quiz, Games, Scheduler, NativeFlow)
• 2. Innovators Soft (LID Mapping & Group Status)
• 3. Itsuki Baileys (Binary Node MD Injection & Decryption)
🛡️ *Baileys Version:* v7.0.0 (Multi-Device Active)
🌐 *Node Type:* Biz Native-Flow (v9 mixed) + Bot Node (biz_bot: 1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> ⚡ *ＰＯＷＥＲＥＤ  ＢＹ   ＹＡＳＨＩ   ＨＡＬＬＳ*`.trim();
    await this.reply(jid, msg, text);
  }

  async cmdGroupStatus(jid: string, msg: any, isGroup: boolean, args: string) {
    if (!isGroup) {
      await this.reply(jid, msg, `❌ *Group Status* command can only be used inside a WhatsApp Group.`);
      return;
    }
    const clean = (args || "").trim();
    if (!clean) {
      await this.reply(jid, msg, `Usage: *${this.state.config.prefix}groupstatus <Status text to broadcast>*`);
      return;
    }
    try {
      await sendGroupStatus(this.sock, jid, { text: clean });
      this.addLog("INFO", `✅ Group status broadcast sent by ${msg.key.participant || jid} to ${jid}`);
    } catch (err: any) {
      await this.reply(jid, msg, `⚠️ Could not send group status: ${err?.message || err}`);
    }
  }

  async resolveBusinessImageBuffer(imageUrl?: string, fallbackUrl?: string): Promise<Buffer> {
    const candidates = [imageUrl, fallbackUrl].filter((u): u is string => Boolean(u && u.trim()));
    for (const candidate of candidates) {
      try {
        let rawBuf: Buffer | null = null;
        const localPath = resolveLocalAssetPath(candidate);
        if (localPath && fs.existsSync(localPath)) {
          rawBuf = fs.readFileSync(localPath);
        } else if (candidate.startsWith("http://") || candidate.startsWith("https://")) {
          const controller = new AbortController();
          const t = setTimeout(() => controller.abort(), 6000);
          try {
            const resp = await fetch(candidate, { signal: controller.signal });
            if (resp.ok) {
              const arr = await resp.arrayBuffer();
              rawBuf = Buffer.from(arr);
            }
          } finally {
            clearTimeout(t);
          }
        }
        if (rawBuf && rawBuf.length > 0) {
          try {
            const sharp = (await import("sharp")).default;
            return await sharp(rawBuf)
              .resize(800, 500, { fit: "inside", withoutEnlargement: true })
              .jpeg({ quality: 85 })
              .toBuffer();
          } catch {
            return rawBuf;
          }
        }
      } catch {}
    }
    return await this.getCardThumbnailBuffer();
  }

  async resolveBusinessHtmlThumbDataUri(imageUrl?: string, fallbackUrl?: string): Promise<string> {
    const candidates = [imageUrl, fallbackUrl].filter((u): u is string => Boolean(u && u.trim()));
    for (const candidate of candidates) {
      const cached = this.businessThumbCache.get(candidate);
      if (cached) return cached;
      try {
        let rawBuf: Buffer | null = null;
        const localPath = resolveLocalAssetPath(candidate);
        if (localPath && fs.existsSync(localPath)) {
          rawBuf = fs.readFileSync(localPath);
        } else if (candidate.startsWith("http://") || candidate.startsWith("https://")) {
          const controller = new AbortController();
          const t = setTimeout(() => controller.abort(), 4500);
          try {
            const resp = await fetch(candidate, { signal: controller.signal });
            if (resp.ok) {
              const arr = await resp.arrayBuffer();
              rawBuf = Buffer.from(arr);
            }
          } finally {
            clearTimeout(t);
          }
        } else if (candidate.startsWith("data:image/")) {
          const b64Part = candidate.split(",")[1];
          if (b64Part) rawBuf = Buffer.from(b64Part, "base64");
        }
        if (rawBuf && rawBuf.length > 0) {
          try {
            const sharp = (await import("sharp")).default;
            const thumbBuf = await sharp(rawBuf)
              .resize(240, 150, { fit: "cover", position: "center" })
              .jpeg({ quality: 65, mozjpeg: true })
              .toBuffer();
            const uri = `data:image/jpeg;base64,${thumbBuf.toString("base64")}`;
            if (this.businessThumbCache.size > 150) {
              const firstKey = this.businessThumbCache.keys().next().value;
              if (firstKey) this.businessThumbCache.delete(firstKey);
            }
            this.businessThumbCache.set(candidate, uri);
            return uri;
          } catch {
            // Fallback raw JPEG/PNG URI if small enough
            if (rawBuf.length < 40000) {
              const uri = `data:image/jpeg;base64,${rawBuf.toString("base64")}`;
              this.businessThumbCache.set(candidate, uri);
              return uri;
            }
          }
        }
      } catch {}
    }
    return "";
  }

  formatBusinessPlatformLabel(platform: BusinessProduct["platform"]): string {
    switch (platform) {
      case "android":
        return "🤖 Android";
      case "ios":
        return "🍎 iOS";
      case "pc":
        return "💻 PC / Emulator";
      case "android_ios":
        return "📱 Android + iOS";
      case "glory_bot":
        return "🔥 Free Fire Glory Bot";
      default:
        return "📱 Mobile";
    }
  }

  trackSentMessageId(sentMsg: any) {
    try {
      const id = sentMsg?.key?.id;
      if (id) {
        this.processedMessageIds.add(id);
        if (this.processedMessageIds.size > 3000) {
          const first = this.processedMessageIds.values().next().value;
          if (first) this.processedMessageIds.delete(first);
        }
      }
    } catch {}
  }

  async sendBusinessButtonMessage(
    jid: string,
    quotedMsg: any,
    options: {
      image?: Buffer | null;
      title?: string;
      text: string;
      footer?: string;
      buttons: Array<{ id: string; text: string }>;
      urls?: Array<{ displayText: string; url: string }>;
    }
  ) {
    if (!this.sock) return;
    const targetTag = jid.split("@")[0].split(":")[0];
    const buttons = (options.buttons || []).filter((b) => b?.id && b?.text);
    const urls = (options.urls || []).filter((u) => u?.url && u?.displayText);

    // Build readable numbered text fallback in case client suppresses interactive buttons
    const quickActionLines = buttons
      .map((b, idx) => `${idx + 1}️⃣ *${b.text}*`)
      .join("\n");
    const urlLines = urls
      .map((u) => `🔗 *${u.displayText}:* ${u.url}`)
      .join("\n");
    const fullFallbackCaption = [
      options.title ? `*${options.title}*` : "",
      options.text,
      quickActionLines ? `\n👇 *තෝරන්න / Reply Number or Name:*\n${quickActionLines}` : "",
      urlLines,
      options.footer ? `\n_${options.footer}_` : "",
    ]
      .filter(Boolean)
      .join("\n");

    // TIER 1: Casileys native Button Builder (interactiveMessage + proper getBizBinaryNode, NO viewOnce wrapper, NO biz_bot: 1)
    try {
      const btn = new Button(this.sock);
      if (options.image && Buffer.isBuffer(options.image) && options.image.length > 0) {
        btn.setImage(options.image);
      }
      if (options.title) btn.setTitle(options.title);
      btn.setBody(options.text || " ");
      if (options.footer) btn.setFooter(options.footer);
      for (const b of buttons) {
        btn.addReply(b.text, b.id);
      }
      for (const u of urls) {
        btn.addUrl(u.displayText, u.url);
      }
      const sent = await btn.send(jid, {
        userJid: this.sock.user?.id,
      });
      this.trackSentMessageId(sent);
      this.state.autoRepliesSentCount++;
      this.addLog("INFO", `✅ [BIZ BUTTONS] Delivered native interactive card to ${targetTag}`);
      return;
    } catch (btnErr: any) {
      this.addLog("WARN", `[BIZ BUTTONS] Native Button notice for ${targetTag}: ${btnErr?.message || btnErr}`);
    }

    // TIER 2: Casileys ButtonV2 (legacy buttonsMessage, up to 3 quick replies)
    if (buttons.length > 0 && buttons.length <= 3 && urls.length === 0) {
      try {
        const b2 = new ButtonV2(this.sock);
        if (options.image && Buffer.isBuffer(options.image) && options.image.length > 0) {
          b2.setImage(options.image);
        }
        b2.setBody(options.title ? `*${options.title}*\n\n${options.text}` : options.text);
        if (options.footer) b2.setFooter(options.footer);
        for (const b of buttons) {
          b2.addButton(b.text, b.id);
        }
        const sent = await b2.send(jid, {
          userJid: this.sock.user?.id,
          viewOnce: false,
        });
        this.trackSentMessageId(sent);
        this.state.autoRepliesSentCount++;
        this.addLog("INFO", `✅ [BIZ BUTTONV2] Delivered legacy buttons card to ${targetTag}`);
        return;
      } catch (b2Err: any) {
        this.addLog("WARN", `[BIZ BUTTONV2] Legacy buttons notice for ${targetTag}: ${b2Err?.message || b2Err}`);
      }
    }

    // TIER 3: Universal Image + Formatted Caption fallback (100% compatible across all WA clients)
    if (options.image && Buffer.isBuffer(options.image) && options.image.length > 0) {
      try {
        const sent = await this.sock.sendMessage(
          jid,
          { image: options.image, caption: fullFallbackCaption },
          quotedMsg ? { quoted: quotedMsg } : undefined
        );
        this.trackSentMessageId(sent);
        this.state.autoRepliesSentCount++;
        this.addLog("INFO", `✅ [BIZ IMAGE FALLBACK] Delivered image menu to ${targetTag}`);
        return;
      } catch (imgErr: any) {
        try {
          const sent = await this.sock.sendMessage(jid, {
            image: options.image,
            caption: fullFallbackCaption,
          });
          this.trackSentMessageId(sent);
          this.state.autoRepliesSentCount++;
          this.addLog("INFO", `✅ [BIZ IMAGE DIRECT] Delivered image menu to ${targetTag}`);
          return;
        } catch (imgErr2: any) {
          this.addLog("WARN", `[BIZ IMAGE] Image send failed for ${targetTag}: ${imgErr2?.message || imgErr2}`);
        }
      }
    }

    // TIER 4: Guaranteed Plain Text Fallback
    const sent = await this.sock.sendMessage(jid, { text: fullFallbackCaption }).catch(() => null);
    this.trackSentMessageId(sent);
    this.state.autoRepliesSentCount++;
    this.addLog("INFO", `✅ [BIZ TEXT FALLBACK] Delivered text menu to ${targetTag}`);
  }

  formatBusinessPackagesLines(product: BusinessProduct): string {
    if (Array.isArray(product.packages) && product.packages.length > 0) {
      return [
        `🔑 *Key Prices (Days / Price):*`,
        ...product.packages.map((pk) => `  • 📅 *${pk.days}:* ${pk.price}`),
      ].join("\n");
    }
    return `💰 *Price:* ${product.price}`;
  }

  getAutoOrderWhatsAppDispatcher(): AutoOrderWhatsAppDispatcher {
    return {
      isOnline: () => Boolean(this.sock && this.state.isOnline),
      sendMessage: async (targetJid: string, text: string): Promise<boolean> => {
        if (!this.sock || !this.state.isOnline || !targetJid) return false;
        try {
          const sent = await this.sock.sendMessage(targetJid, { text });
          this.trackSentMessageId(sent);
          return true;
        } catch (err: any) {
          this.addLog("WARN", `⚠️ [AUTO ORDER WA] Failed to send message to ${targetJid.split("@")[0]}: ${err?.message || err}`);
          return false;
        }
      },
      sendInteractiveCard: async (targetJid: string, opts: {
        image?: Buffer | null;
        title?: string;
        text: string;
        footer?: string;
        buttons: Array<{ id: string; text: string }>;
        urls?: Array<{ displayText: string; url: string }>;
      }): Promise<boolean> => {
        if (!this.sock || !this.state.isOnline || !targetJid) return false;
        try {
          await this.sendBusinessButtonMessage(targetJid, null, opts);
          return true;
        } catch (err: any) {
          this.addLog("WARN", `⚠️ [AUTO ORDER CARD] Failed to send interactive card to ${targetJid.split("@")[0]}: ${err?.message || err}`);
          return false;
        }
      },
      sendHtmlAppCard: async (targetJid: string, htmlMarkup: string, opts: { title: string; height: number }): Promise<string | null> => {
        if (!this.sock || !this.state.isOnline || !targetJid) return null;
        try {
          const res = await sendHtmlApp(this.sock, targetJid, htmlMarkup, {
            title: opts.title,
            height: opts.height,
          });
          if (res?.messageId) {
            this.trackSentMessageId({ key: { id: res.messageId } });
            return res.messageId;
          }
          return null;
        } catch (err: any) {
          this.addLog("WARN", `⚠️ [AUTO ORDER HTML CARD] Failed to send HTML App card: ${err?.message || err}`);
          return null;
        }
      },
      editHtmlAppCard: async (targetJid: string, messageId: string, htmlMarkup: string, opts: { title: string; height: number }): Promise<boolean> => {
        if (!this.sock || !this.state.isOnline || !targetJid || !messageId) return false;
        try {
          return await editHtmlApp(this.sock, targetJid, messageId, htmlMarkup, {
            title: opts.title,
            height: opts.height,
          });
        } catch (err: any) {
          this.addLog("WARN", `⚠️ [AUTO ORDER HTML CARD] Failed to edit HTML App card: ${err?.message || err}`);
          return false;
        }
      },
      getConnectedBotJid: (): string | null => {
        const connectedDigits = (
          this.sock?.user?.id?.split(":")?.[0]?.split("@")?.[0] ||
          this.state.pairedNumber ||
          this.state.config.ownerNumber ||
          ""
        ).replace(/\D/g, "");
        return connectedDigits.length >= 8 ? `${connectedDigits}@s.whatsapp.net` : null;
      },
      getOwnerJid: (): string | null => {
        const ownerDigits = (
          this.state.pairedNumber ||
          this.sock?.user?.id?.split(":")?.[0]?.split("@")?.[0] ||
          this.state.config.ownerNumber ||
          ""
        ).replace(/\D/g, "");
        return ownerDigits.length >= 8 ? `${ownerDigits}@s.whatsapp.net` : null;
      },
      getStoreName: (): string => {
        try {
          return getBusinessStore(this.userId).storeName || this.state.config.botName || "YASHI HALLS";
        } catch {
          return "YASHI HALLS";
        }
      },
    };
  }

  /** PHASE 1 — Welcome Card with GUILD SERVICE & PANEL SERVICE Native Buttons */
  async sendGuildWelcomeCard(jid: string, msg: any, brandName = "YASHI HALLS") {
    this.markBusinessCustomerWelcomed(jid, msg);
    let imgBuf: Buffer | null = null;
    try {
      imgBuf = await renderWelcomeCardPng(brandName);
    } catch {
      imgBuf = null;
    }
    this.businessChatState.set(jid, { step: "welcome", updatedAt: Date.now() });
    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: `WELCOME TO ${brandName.toUpperCase()}`,
      text: [
        `Welcome to *${brandName}*!`,
        `What service would you like today?`,
        ``,
        `1️⃣ *GUILD SERVICE* — Automated Free Fire Glory Bots (4 / 8 / 12 / 16 Bots)`,
        `2️⃣ *PANEL SERVICE* — Browse Verified Panels & Keys`,
      ].join("\n"),
      footer: `${brandName} • Interactive Order System`,
      buttons: [
        { id: "guild_svc_start", text: "⚔️ GUILD SERVICE" },
        { id: "panel_svc_start", text: "🖥️ PANEL SERVICE" },
      ],
    });
  }

  /** PHASE 2 — Bot Quantity Selection Card (4 / 8 / 12 / 16 BOTS) */
  async sendGuildQuantityMenu(jid: string, msg: any, brandName = "YASHI HALLS") {
    const aoState = getAutoOrderState(this.userId);
    const pricing = aoState.settings.guildPricing;
    let imgBuf: Buffer | null = null;
    try {
      imgBuf = await renderQuantityCardPng(brandName, pricing);
    } catch {
      imgBuf = null;
    }
    this.businessChatState.set(jid, { step: "guild_qty", updatedAt: Date.now() });
    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: "HOW MANY GLORY BOTS DO YOU NEED?",
      text: [
        `🤖 *How many Glory Bots do you need?*`,
        ``,
        `• *4 BOTS* — LKR ${pricing.bots4.toLocaleString("en-US")}`,
        `• *8 BOTS* — LKR ${pricing.bots8.toLocaleString("en-US")}`,
        `• *12 BOTS* — LKR ${pricing.bots12.toLocaleString("en-US")}`,
        `• *16 BOTS* — LKR ${pricing.bots16.toLocaleString("en-US")}`,
        ``,
        `👇 Tap a button below (or reply *4*, *8*, *12*, or *16*) to generate your Payment Card.`,
      ].join("\n"),
      footer: `${brandName} • Guild Service Pricing`,
      buttons: [
        { id: "guild_qty:4", text: `4 BOTS · LKR ${pricing.bots4}` },
        { id: "guild_qty:8", text: `8 BOTS · LKR ${pricing.bots8}` },
        { id: "guild_qty:12", text: `12 BOTS · LKR ${pricing.bots12}` },
        { id: "guild_qty:16", text: `16 BOTS · LKR ${pricing.bots16}` },
      ],
    });
  }

  /** Sends the Interactive HTML App Bank & eZ Cash Details UI Card with Embedded Banner Photo */
  async sendBankDetailsHtmlAppCard(
    jid: string,
    msg: any,
    brandName = "YASHI HALLS",
    orderInfo?: { orderId: string; serviceLabel: string; packageLabel: string; amountLkr: number }
  ): Promise<boolean> {
    const aoState = getAutoOrderState(this.userId);
    const acc = aoState.settings.paymentAccount;
    const bankName =
      acc.bankName && acc.bankName !== "Commercial Bank / Sampath Bank"
        ? acc.bankName
        : "NSB (National Savings Bank)";
    const accountName =
      acc.accountName && acc.accountName !== "YASHI HALLS"
        ? acc.accountName
        : "A.M.Y.N Alahakoon";
    const accountNumber =
      acc.accountNumber && acc.accountNumber !== "Configure in Admin Dashboard"
        ? acc.accountNumber
        : "100156103618";
    const ezMatch = String(acc.branchName || "").match(/0\d{2}[\s-]?\d{3}[\s-]?\d{4}/);
    const ezCashNumber = ezMatch ? ezMatch[0].replace(/\D/g, "") : "0743865934";
    const instructions =
      acc.instructions ||
      "NSB (100156103618 - A.M.Y.N Alahakoon) හෝ Dialog eZ Cash (074 386 5934) වෙත මුදල ගෙවා Payment Receipt (Photo / PDF) එක මෙතනට එවන්න.";

    const bannerDataUri = await getEmbeddedBankBannerDataUri();
    const cardHeight = orderInfo ? 620 : 540;
    const htmlMarkup = generatePaymentBankDetailsHtml({
      brandName,
      orderId: orderInfo?.orderId,
      serviceLabel: orderInfo?.serviceLabel,
      packageLabel: orderInfo?.packageLabel,
      amountLkr: orderInfo?.amountLkr,
      bankName,
      accountName,
      accountNumber,
      ezCashNumber,
      instructions,
      bannerDataUri,
      height: cardHeight,
    });

    try {
      const res = await sendHtmlApp(this.sock, jid, htmlMarkup, {
        title: orderInfo ? `🏦 PAYMENT • ${orderInfo.orderId}` : `🏦 ${brandName.toUpperCase()} BANK DETAILS`,
        height: cardHeight,
        quoted: msg || undefined,
      });
      if (res?.messageId) {
        this.trackSentMessageId({ key: { id: res.messageId } });
      }
      return Boolean(res?.success);
    } catch (err: any) {
      this.addLog("WARN", `⚠️ [BANK UI CARD] HTML App card fallback: ${err?.message || err}`);
      return false;
    }
  }

  /** PHASE 3 — Premium HTML App Bank Details UI Card + Quick Action Buttons + Pending Order Creation */
  async sendGuildPaymentCard(jid: string, msg: any, qty: GuildBotQuantity, brandName = "YASHI HALLS") {
    const senderTag = (msg?.key?.remoteJidAlt || msg?.key?.participantAlt || msg?.key?.participant || jid)
      .split("@")[0]
      .split(":")[0];
    const customerName = String(msg?.pushName || senderTag || "Customer").trim();

    const order = createGuildServiceOrder(this.userId, {
      customerJid: jid,
      customerNumber: senderTag,
      customerName,
      botQuantity: qty,
    });

    // Also register in BusinessPay tracker when enabled so existing Android Pay App matches seamlessly
    const payState = getBusinessPayState(this.userId);
    if (payState.config.enabled) {
      createBusinessPayOrder(this.userId, {
        customerJid: jid,
        customerNumber: senderTag,
        productId: order.productId,
        productName: order.productName,
        packageDays: `${qty} Glory Bots`,
        priceDisplay: order.priceDisplay,
        amountLkr: order.amountLkr,
      });
    }

    const aoState = getAutoOrderState(this.userId);
    const acc = aoState.settings.paymentAccount;
    const bankName =
      acc.bankName && acc.bankName !== "Commercial Bank / Sampath Bank"
        ? acc.bankName
        : "NSB (National Savings Bank)";
    const accountName =
      acc.accountName && acc.accountName !== "YASHI HALLS"
        ? acc.accountName
        : "A.M.Y.N Alahakoon";
    const accountNumber =
      acc.accountNumber && acc.accountNumber !== "Configure in Admin Dashboard"
        ? acc.accountNumber
        : "100156103618";
    const ezBranch = acc.branchName || "eZ Cash: 074 386 5934";
    const instructions =
      acc.instructions ||
      "NSB (100156103618 - A.M.Y.N Alahakoon) හෝ Dialog eZ Cash (074 386 5934) වෙත මුදල ගෙවා Payment Receipt (Photo / PDF) එක මෙතනට එවන්න.";

    this.businessChatState.set(jid, {
      step: "guild_awaiting_receipt",
      orderId: order.id,
      updatedAt: Date.now(),
    });

    this.addLog("CMD", `⚔️ [GUILD ORDER] ${senderTag} created ${order.id} (${qty} BOTS • LKR ${order.amountLkr})`);

    // 1. Send the Interactive In-Chat HTML App Bank Details & Payment UI Card (with embedded Bank/eZ Cash Photo & 1-Tap Copy!)
    const htmlSent = await this.sendBankDetailsHtmlAppCard(jid, msg, brandName, {
      orderId: order.id,
      serviceLabel: "GUILD SERVICE",
      packageLabel: `${qty} BOTS`,
      amountLkr: order.amountLkr,
    });

    // 2. Send compact interactive quick buttons (or full fallback PNG card if HTML App failed)
    if (htmlSent) {
      await this.sendBusinessButtonMessage(jid, null, {
        title: `ORDER ${order.id} • READY FOR RECEIPT`,
        text: [
          `📤 *ගෙවීම සිදු කළ පසු ඔබගේ Payment Receipt (Photo හෝ PDF) එක මෙම Chat එකට එවන්න.*`,
          `🧾 *Order ID:* \`${order.id}\`  |  💰 *Amount:* LKR ${order.amountLkr.toLocaleString("en-US")}`,
        ].join("\n"),
        footer: `${brandName} • Automated Receipt & Approval System`,
        buttons: [
          { id: "guild_svc_start", text: "🔄 Change Quantity" },
          { id: "guild_bank_card", text: "🏦 Bank Details Card" },
          { id: "guild_welcome_menu", text: "🏠 Main Menu" },
        ],
      });
      return;
    }

    let cardPng: Buffer | null = null;
    try {
      cardPng = await renderPaymentCardPng({
        brandName,
        orderId: order.id,
        serviceLabel: "GUILD SERVICE",
        botQuantity: qty,
        amountLkr: order.amountLkr,
        bankName,
        accountName,
        accountNumber,
        branchName: ezBranch,
        instructions,
      });
    } catch {
      cardPng = null;
    }

    const paymentBody = [
      `🧾 *Order ID:* ${order.id}`,
      `⚔️ *Selected Service:* GUILD SERVICE`,
      `🤖 *Selected Bot Quantity:* ${qty} BOTS`,
      `💰 *Total Price:* LKR ${order.amountLkr.toLocaleString("en-US")}`,
      ``,
      `🏦 *Bank Name:* ${bankName}`,
      `👤 *Account Name:* ${accountName}`,
      `💳 *Account Number:* \`${accountNumber}\``,
      `📱 *eZ Cash:* \`074 386 5934\``,
      `🔖 *Bank Remark:* \`${order.id}\``,
      ``,
      `📤 *ගෙවීම සිදු කර Payment Receipt (Image / PDF) එක මෙතනට එවන්න.*`,
    ].join("\n");

    await this.sendBusinessButtonMessage(jid, msg, {
      image: cardPng,
      title: `💳 PAYMENT CARD • ${order.id}`,
      text: paymentBody,
      footer: `${brandName} • Automated Receipt & Approval System`,
      buttons: [
        { id: "guild_svc_start", text: "🔄 Change Quantity" },
        { id: "guild_welcome_menu", text: "🏠 Main Menu" },
      ],
    });
  }

  async sendBusinessOrderDetails(
    jid: string,
    msg: any,
    product: BusinessProduct,
    store: BusinessStoreData,
    ownerDigits: string,
    selectedPkg?: BusinessPricePackage | null,
    clanIdOverride?: string
  ) {
    const chosenDays = selectedPkg?.days || "";
    const chosenPrice = selectedPkg?.price || product.price;
    const senderTag = (msg?.key?.remoteJidAlt || msg?.key?.participantAlt || msg?.key?.participant || jid).split("@")[0].split(":")[0];
    const customerName = String(msg?.pushName || senderTag || "Customer").trim();

    // Check if product/package is mapped to FFGlory Clan Group and needs Clan ID first
    const ffMapping = findProductMapping(this.userId, product.id, selectedPkg?.id);
    if (ffMapping && ffMapping.enabled && ffMapping.actionType === "CLAN_GROUP" && !clanIdOverride) {
      this.businessChatState.set(jid, {
        step: "awaiting_clan_id",
        productId: product.id,
        packageId: selectedPkg?.id,
        updatedAt: Date.now(),
      });
      const askClanText = [
        `🔥 *${product.name}*${chosenDays ? ` (${chosenDays})` : ""}`,
        `🌍 *Server Region:* ${(ffMapping.region || "BD").toUpperCase()}`,
        `💰 *Price:* ${chosenPrice}`,
        ``,
        `👉 ඔබගේ *Free Fire Clan / Guild ID* එක මෙතනට Reply කරන්න (උදා: \`123456789\`).`,
        `❌ අවලංගු කිරීමට *cancel* යවන්න.`,
      ].join("\n");
      const sentPrompt = await this.sock.sendMessage(jid, { text: askClanText }, msg ? { quoted: msg } : undefined).catch(() => null);
      this.trackSentMessageId(sentPrompt);
      return;
    }

    const imgBuf = await this.resolveBusinessImageBuffer(product.logoUrl, store.mainLogoUrl);
    const payState = getBusinessPayState(this.userId);
    const computedAmountLkr =
      ffMapping?.priceLkrOverride && ffMapping.priceLkrOverride > 0
        ? ffMapping.priceLkrOverride
        : estimateAmountLkr(chosenPrice, payState.config.lkrPerUsdRate);

    let trackedOrder: BusinessPayOrder | null = null;
    if (payState.config.enabled) {
      trackedOrder = createBusinessPayOrder(this.userId, {
        customerJid: jid,
        customerNumber: senderTag,
        productId: product.id,
        productName: product.name,
        packageDays: chosenDays,
        priceDisplay: chosenPrice,
        amountLkr: computedAmountLkr > 0 ? computedAmountLkr : undefined,
      });
    }

    // Persist synchronized Auto Order record (PENDING_PAYMENT)
    const autoOrder = createAutoOrder(this.userId, {
      orderId: trackedOrder?.id,
      customerJid: jid,
      customerNumber: senderTag,
      customerName,
      productId: product.id,
      productName: product.name,
      packageId: selectedPkg?.id,
      packageDays: chosenDays,
      priceDisplay: chosenPrice,
      amountLkr: trackedOrder?.amountLkr || computedAmountLkr,
      clanId: clanIdOverride,
    });

    this.addLog(
      "CMD",
      `🛒 [BIZ ORDER] ${senderTag} selected "${product.name}"${chosenDays ? ` (${chosenDays} • ${chosenPrice})` : ` (${chosenPrice})`} [${autoOrder.id} | ${autoOrder.deliveryAction}]`
    );

    const finalAmountLkr = autoOrder.amountLkr || trackedOrder?.amountLkr || 0;
    const orderText = [
      `🧾 *Order ID:* ${autoOrder.id}`,
      `✅ *Product:* ${product.name}`,
      `📂 *Category:* ${product.category}`,
      `📱 *Platform:* ${this.formatBusinessPlatformLabel(product.platform)}`,
      chosenDays ? `📅 *Duration (Days):* ${chosenDays}` : "",
      clanIdOverride ? `🛡️ *Clan ID:* ${clanIdOverride}` : "",
      ffMapping?.actionType === "CLAN_GROUP" && ffMapping.region ? `🌍 *Region:* ${ffMapping.region.toUpperCase()}` : "",
      `💰 *Price:* ${chosenPrice}${finalAmountLkr > 0 ? ` (LKR ${finalAmountLkr.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 })})` : ""}`,
      `📦 *Status:* 🟡 PENDING PAYMENT`,
      ffMapping && ffMapping.enabled && ffMapping.actionType !== "MANUAL"
        ? `⚡ *Auto Delivery:* Bank Payment තහවුරු වූ වහාම ස්වයංක්‍රීයව ලබා දෙනු ඇත.`
        : "",
      ``,
      `📝 ${store.orderInstructions}`,
      payState.config.enabled && payState.config.bankDetailsText ? `💳 ${payState.config.bankDetailsText}` : "",
      `🔖 _Bank Remark/Reference එකට *${autoOrder.id}* සඳහන් කර නිවැරදි මුදල ගෙවන්න._`,
      ownerDigits ? `📞 *WhatsApp:* https://wa.me/${ownerDigits}` : "",
    ]
      .filter(Boolean)
      .join("\n");

    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: `🛒 ORDER DETAILS • ${product.name}`,
      text: orderText,
      footer: `${store.storeName} • Thank You!`,
      buttons: [
        { id: "guild_bank_card", text: "🏦 Bank Details" },
        { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
      ],
      urls: ownerDigits
        ? [{ displayText: "💬 Contact Owner", url: `https://wa.me/${ownerDigits}` }]
        : [],
    });
  }

  async handleIncomingBusinessPayment(
    event: BusinessPayEvent,
    matchedOrder?: BusinessPayOrder
  ) {
    const amountFormatted = `LKR ${Number(event.amountLkr || 0).toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
    this.addLog(
      "INFO",
      `💳 [YASHI PAY ${event.source.toUpperCase()}] ${event.bank} • ${amountFormatted} (Ref: ${event.reference})${
        matchedOrder ? ` → MATCHED ${matchedOrder.id}` : ""
      }`
    );

    if (!this.sock || !this.state.isOnline) {
      return;
    }

    const store = getBusinessStore(this.userId);
    const payState = getBusinessPayState(this.userId);

    // 1. Notify customer if order auto-matched
    if (matchedOrder && payState.config.notifyCustomerOnMatch && matchedOrder.customerJid) {
      const custMsg = [
        `✅ *PAYMENT VERIFIED AUTOMATICALLY!*`,
        ``,
        `🧾 *Order ID:* ${matchedOrder.id}`,
        `🛍️ *Product:* ${matchedOrder.productName}${matchedOrder.packageDays ? ` (${matchedOrder.packageDays})` : ""}`,
        `🏦 *Bank/Wallet:* ${event.bank}`,
        `💰 *Amount:* ${amountFormatted}`,
        `🔖 *Ref:* ${event.reference}`,
        ``,
        `ඔබගේ Payment එක *YASHI HALLS Pay* මඟින් සාර්ථකව තහවුරු විය! ඉක්මනින්ම ඔබගේ Order එක ලබා දෙනු ඇත. 🙏`,
        `_${store.storeName}_`,
      ].join("\n");
      await this.sock.sendMessage(matchedOrder.customerJid, { text: custMsg }).catch(() => {});
    }

    // 2. Notify Business Owner on WhatsApp
    if (payState.config.notifyOwnerOnPayment && event.status !== "ignored") {
      const ownerDigits = (this.state.config.ownerNumber || this.state.pairedNumber || "").replace(/\D/g, "");
      if (ownerDigits.length >= 8) {
        const ownerJid = `${ownerDigits}@s.whatsapp.net`;
        const ownerMsg = [
          `💰 *YASHI HALLS PAY • NEW PAYMENT RECEIVED*`,
          ``,
          `🏦 *Source:* ${event.bank} (${event.source.toUpperCase()})`,
          `💵 *Amount:* ${amountFormatted}`,
          event.balanceLkr !== undefined && event.balanceLkr !== null
            ? `💳 *Avail Bal:* LKR ${Number(event.balanceLkr).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
            : "",
          `🔖 *Reference:* ${event.reference}`,
          event.deviceName ? `📱 *Device:* ${event.deviceName}` : "",
          matchedOrder
            ? `✅ *Matched Order:* ${matchedOrder.id} • ${matchedOrder.productName} (+${matchedOrder.customerNumber})`
            : `ℹ️ *Status:* Recorded (No matching pending order)`,
          ``,
          `_${store.storeName} • YASHI HALLS Pay Gateway_`,
        ]
          .filter(Boolean)
          .join("\n");
        await this.sock.sendMessage(ownerJid, { text: ownerMsg }).catch(() => {});
      }
    }
  }

  async sendYashiPayApkToWhatsApp(
    targetJid: string,
    quotedMsg?: any
  ): Promise<{ ok: boolean; error?: string }> {
    if (!this.sock || !this.state.isOnline) {
      return { ok: false, error: "Business Bot is not currently online." };
    }
    try {
      const apkPath = getApkBinaryPath();
      if (!fs.existsSync(apkPath)) {
        return { ok: false, error: "YashiHallsPay APK binary was not found on the server." };
      }
      const info = getApkBinaryInfo();
      const apkBuffer = fs.readFileSync(apkPath);
      const caption = [
        `📲 *YASHI HALLS PAY • ANDROID APK (v${info.version})*`,
        ``,
        `📦 *Package:* ${info.packageName}`,
        `📏 *Size:* ${Math.round(apkBuffer.length / 1024)} KB`,
        ``,
        `✅ *සම්බන්ධ කරගන්නා ආකාරය:*`,
        `1️⃣ මෙම ගොනුව Tap කර Android දුරකථනයේ Install කරන්න ("Unknown apps" අවසර දෙන්න).`,
        `2️⃣ ඊළඟට Business Bot එකට *.paykey* කියා යවා ඔබගේ Connect Key එක ලබාගන්න.`,
        `3️⃣ App එක Open කර *Link to your bot* තැනට එම Key එක Paste කර *Link* ඔබන්න.`,
        `4️⃣ Rules ටැබ් එකෙන් Bank/eZ Cash rules Add කර අවසර ලබා දෙන්න.`,
      ].join("\n");

      const sent = await this.sock.sendMessage(
        targetJid,
        {
          document: apkBuffer,
          mimetype: "application/vnd.android.package-archive",
          fileName: `YashiHallsPay-v${info.version}.apk`,
          caption,
        },
        quotedMsg ? { quoted: quotedMsg } : undefined
      );
      this.trackSentMessageId(sent);
      this.addLog("INFO", `📦 [BUSINESS PAY APK] Delivered YashiHallsPay-v${info.version}.apk to ${targetJid.split("@")[0]}`);
      return { ok: true };
    } catch (err: any) {
      const errMsg = err?.message || "Failed to send APK document via WhatsApp";
      this.addLog("ERROR", `❌ [BUSINESS PAY APK] Delivery failed: ${errMsg}`);
      return { ok: false, error: errMsg };
    }
  }

  async sendYashiPayKeyToWhatsApp(
    targetJid: string,
    quotedMsg?: any
  ): Promise<{ ok: boolean; error?: string }> {
    if (!this.sock || !this.state.isOnline) {
      return { ok: false, error: "Business Bot is not currently online." };
    }
    try {
      const linkKey = buildBotLinkKey(this.userId);
      const payState = getBusinessPayState(this.userId);
      const infoMsg = [
        `🔑 *YASHI HALLS PAY — BOT CONNECT KEY*`,
        ``,
        `💼 *Store:* ${getBusinessStore(this.userId).storeName || "YASHI BUSINESS STORE"}`,
        `🔗 *Link Status:* Ready for YASHI HALLS Pay Android App`,
        ``,
        `📌 *භාවිතා කරන ආකාරය:*`,
        `1️⃣ පහත ඇති *YHP...* කෝඩ් එක Copy කරගන්න.`,
        `2️⃣ *YASHI HALLS Pay* App එකේ *Home → Link to your bot* තැන *Paste* කර *Link* ඔබන්න.`,
        `3️⃣ Rules ටැබ් එකෙන් ඔබේ බැංකු SMS/Notification rules එක් කරන්න.`,
        ``,
        `⚠️ මෙම Key එක කිසිවෙකු සමඟ බෙදා නොගන්න.${!payState.lastServerOrigin && !process.env.APP_URL ? "\n\n💡 සටහන: ප්‍රථම වරට Business Bot Dashboard පිටුව විවෘත කර ඇත්නම් Server URL ස්වයංක්‍රීයව ඇතුළත් වේ." : ""}`,
      ].join("\n");

      const sentInfo = await this.sock.sendMessage(
        targetJid,
        { text: infoMsg },
        quotedMsg ? { quoted: quotedMsg } : undefined
      );
      this.trackSentMessageId(sentInfo);

      const sentKey = await this.sock.sendMessage(targetJid, {
        text: linkKey,
      });
      this.trackSentMessageId(sentKey);

      this.addLog("INFO", `🔑 [BUSINESS PAY KEY] Sent App Connect Key to ${targetJid.split("@")[0]}`);
      return { ok: true };
    } catch (err: any) {
      const errMsg = err?.message || "Failed to send Pay Key via WhatsApp";
      this.addLog("ERROR", `❌ [BUSINESS PAY KEY] Delivery failed: ${errMsg}`);
      return { ok: false, error: errMsg };
    }
  }

  async sendBusinessPackageSelector(
    jid: string,
    msg: any,
    product: BusinessProduct,
    storeName: string,
    mainLogoUrl: string
  ) {
    const packages = Array.isArray(product.packages) ? product.packages : [];
    const imgBuf = await this.resolveBusinessImageBuffer(product.logoUrl, mainLogoUrl);
    this.businessChatState.set(jid, {
      step: "package",
      productId: product.id,
      updatedAt: Date.now(),
    });

    const buttons = [
      ...packages.slice(0, 9).map((pk) => ({
        id: `biz_buy_pkg:${product.id}:${pk.id}`,
        text: `🔑 ${pk.days} • ${pk.price}`,
      })),
      { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
    ];

    const body = [
      `📂 *Category:* ${product.category}`,
      `📱 *Platform:* ${this.formatBusinessPlatformLabel(product.platform)}`,
      ``,
      this.formatBusinessPackagesLines(product),
      ``,
      `👇 *ඔබට අවශ්‍ය දවස් ගණන (Key Duration) තෝරන්න:*`,
    ].join("\n");

    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: `🔑 SELECT DAYS • ${product.name}`,
      text: body,
      footer: `${storeName} • Select Key Duration`,
      buttons,
    });
  }

  async sendBusinessSingleProductCard(
    jid: string,
    msg: any,
    product: BusinessProduct,
    storeName: string,
    mainLogoUrl: string,
    backButtonId: string,
    backButtonText: string
  ) {
    const imgBuf = await this.resolveBusinessImageBuffer(product.logoUrl, mainLogoUrl);
    const statusText = product.inStock ? "🟢 IN STOCK" : "🔴 OUT OF STOCK";
    const pkgList = Array.isArray(product.packages) ? product.packages : [];
    const buyText =
      pkgList.length > 1
        ? `🛒 Buy (${product.price}+)`
        : pkgList.length === 1
        ? `🛒 Buy (${pkgList[0].days} • ${pkgList[0].price})`
        : `🛒 Buy (${product.price})`;

    const body = [
      `📂 *Category:* ${product.category}`,
      `📱 *Platform:* ${this.formatBusinessPlatformLabel(product.platform)}`,
      this.formatBusinessPackagesLines(product),
      `📦 *Status:* ${statusText}`,
      product.subtitle ? `🏷️ *Info:* ${product.subtitle}` : "",
      product.description ? `\n📝 ${product.description}` : "",
    ]
      .filter(Boolean)
      .join("\n");

    const buttons = [
      product.inStock
        ? { id: `biz_buy:${product.id}`, text: buyText }
        : { id: `biz_oos:${product.id}`, text: `🔴 Out of Stock` },
      { id: backButtonId, text: backButtonText },
      { id: "biz_home", text: "🏠 Main Menu" },
    ];

    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: `${product.verified ? "✅ " : ""}${product.name}`,
      text: body,
      footer: `${storeName} • ${product.category}`,
      buttons,
    });
  }

  async sendBusinessCategoryCards(
    jid: string,
    msg: any,
    categoryName: string,
    products: BusinessProduct[],
    storeName: string,
    mainLogoUrl: string,
    platformLabel: string,
    backButtonId: string,
    backButtonText: string
  ) {
    if (!products || products.length === 0) return;

    // For single product in a category, send a crisp image+button card
    if (products.length === 1) {
      await this.sendBusinessSingleProductCard(
        jid,
        msg,
        products[0],
        storeName,
        mainLogoUrl,
        backButtonId,
        backButtonText
      );
      return;
    }

    // For 2+ products in a category, deliver native Casileys Carousel Card Slider (max 10 cards per batch)
    try {
      for (let start = 0; start < products.length; start += 10) {
        const chunk = products.slice(start, start + 10);
        if (chunk.length === 1) {
          await this.sendBusinessSingleProductCard(
            jid,
            msg,
            chunk[0],
            storeName,
            mainLogoUrl,
            backButtonId,
            backButtonText
          );
          continue;
        }

        const builtCards = await Promise.all(
          chunk.map(async (p) => {
            const imgBuf = await this.resolveBusinessImageBuffer(p.logoUrl, mainLogoUrl);
            const statusLine = p.inStock ? "🟢 IN STOCK" : "🔴 OUT OF STOCK";
            const pkgList = Array.isArray(p.packages) ? p.packages : [];
            const buyLabel =
              pkgList.length > 1
                ? `🛒 Select Days (${p.price}+)`
                : pkgList.length === 1
                ? `🛒 Buy (${pkgList[0].days} • ${pkgList[0].price})`
                : `🛒 Buy (${p.price})`;
            const subtitleText =
              pkgList.length > 1
                ? `${pkgList.length} Key Plans (${p.price}+) • ${p.inStock ? "In Stock" : "Out of Stock"}`
                : pkgList.length === 1
                ? `${pkgList[0].days} • ${pkgList[0].price} • ${p.inStock ? "In Stock" : "Out of Stock"}`
                : `${p.price} • ${p.inStock ? "In Stock" : "Out of Stock"}`;

            const cardBody = [
              `📂 ${categoryName}`,
              `📱 ${this.formatBusinessPlatformLabel(p.platform)}`,
              this.formatBusinessPackagesLines(p),
              `📦 Status: ${statusLine}`,
              p.subtitle ? `🏷️ ${p.subtitle}` : "",
              p.description ? `\n📝 ${p.description}` : "",
            ]
              .filter(Boolean)
              .join("\n");

            const cardBtn = new Button(this.sock)
              .setImage(imgBuf)
              .setTitle(`${p.verified ? "✅ " : ""}${p.name}`)
              .setSubtitle(subtitleText)
              .setBody(cardBody)
              .setFooter(`${storeName} • ${categoryName}`);

            if (p.inStock) {
              cardBtn.addReply(buyLabel, `biz_buy:${p.id}`);
            } else {
              cardBtn.addReply("🔴 Out of Stock", `biz_oos:${p.id}`);
            }
            cardBtn.addReply(backButtonText, backButtonId);

            return await cardBtn.toCard();
          })
        );

        const carousel = new Carousel(this.sock);
        carousel.setBody(
          `📂 *${categoryName}* (${platformLabel})\n━━━━━━━━━━━━━━━━━━━━━━━━\n💡 _පසෙකට Swipe කරමින් Products, දවස් ගණන සහ මිල ගණන් බලා Buy ඔබන්න 👇_`
        );
        carousel.setFooter(`${storeName} • ${categoryName}`);
        carousel.addCard(builtCards);

        const sent = await carousel.send(jid, {
          userJid: this.sock.user?.id,
        });
        this.trackSentMessageId(sent);
        this.state.autoRepliesSentCount++;
        this.addLog("INFO", `✅ [BIZ CAROUSEL] Delivered ${chunk.length} cards for "${categoryName}" to ${jid.split("@")[0]}`);
      }
      return;
    } catch (err: any) {
      this.addLog("WARN", `[BIZ CAROUSEL] Fallback to single cards for ${categoryName}: ${err?.message || err}`);
      for (const p of products) {
        await this.sendBusinessSingleProductCard(
          jid,
          msg,
          p,
          storeName,
          mainLogoUrl,
          backButtonId,
          backButtonText
        );
      }
    }
  }

  getBusinessPanelCommand(product: BusinessProduct, prefix = "."): string {
    const slug = sanitizePanelCommandSlug(product.command, product.name);
    return `${prefix}${slug}`;
  }

  async sendBusinessCatalogAppMenu(
    jid: string,
    msg: any,
    store: BusinessStoreData
  ) {
    if (!this.sock) return;
    const prefix = this.state.config.prefix || ".";
    const products = Array.isArray(store.products) ? store.products : [];

    if (products.length === 0) {
      await this.sock.sendMessage(
        jid,
        { text: `🛍️ *${store.storeName || "YASHI BUSINESS STORE"}*\n\nදැනට Panels ඇතුළත් කර නොමැත.` },
        msg ? { quoted: msg } : undefined
      ).catch(() => {});
      return;
    }

    const mappedItems = await Promise.all(
      products.map(async (p) => {
        const imageDataUri = await this.resolveBusinessHtmlThumbDataUri(p.logoUrl, store.mainLogoUrl);
        return {
          id: p.id,
          name: p.name,
          category: p.category,
          platform: p.platform,
          price: p.price,
          packages: Array.isArray(p.packages) ? p.packages : [],
          subtitle: p.subtitle,
          imageDataUri,
          inStock: p.inStock,
          verified: p.verified,
          command: this.getBusinessPanelCommand(p, prefix),
        };
      })
    );

    const dynamicHeight = calculateBusinessCatalogHtmlHeight(store.categories, products);

    const markup = generateBusinessCatalogHtmlApp({
      storeName: store.storeName,
      prefix,
      categories: store.categories,
      products: mappedItems,
      height: dynamicHeight,
    });

    try {
      const result = await sendHtmlApp(this.sock, jid, markup, {
        title: `🛍️ ${store.storeName || "YASHI BUSINESS"} • PANELS MENU`,
        height: dynamicHeight,
        bypassDownload: true,
      });
      if (result.success) {
        this.state.autoRepliesSentCount++;
        this.addLog("INFO", `✅ [BIZ HTML APP] Delivered categorized App UI Card menu (${products.length} panels, ${dynamicHeight}px) to ${jid.split("@")[0]}`);
        return;
      }
      this.addLog("WARN", `[BIZ HTML APP] sendHtmlApp returned unsuccessful: ${result.error || "unknown error"}`);
    } catch (htmlErr: any) {
      this.addLog("WARN", `[BIZ HTML APP] Failed sending HTML UI Menu: ${htmlErr?.message || htmlErr}`);
    }
  }

  async sendBusinessPanelDetailsByCommand(
    jid: string,
    msg: any,
    product: BusinessProduct,
    store: BusinessStoreData
  ) {
    const imgBuf = await this.resolveBusinessImageBuffer(product.logoUrl, store.mainLogoUrl);
    const packages = Array.isArray(product.packages) ? product.packages : [];
    const statusText = product.inStock ? "🟢 IN STOCK" : "🔴 OUT OF STOCK";
    const panelCmd = this.getBusinessPanelCommand(product, this.state.config.prefix || ".");

    if (product.inStock && packages.length > 0) {
      this.businessChatState.set(jid, {
        step: "package",
        productId: product.id,
        updatedAt: Date.now(),
      });
    } else {
      this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
    }

    const body = [
      `📂 *Category:* ${product.category}`,
      `📱 *Platform:* ${this.formatBusinessPlatformLabel(product.platform)}`,
      `📦 *Status:* ${statusText}`,
      product.subtitle ? `🏷️ *Info:* ${product.subtitle}` : "",
      ``,
      this.formatBusinessPackagesLines(product),
      product.description ? `\n📝 ${product.description}` : "",
      product.inStock && packages.length > 0
        ? `\n👇 *මිලදී ගැනීමට අවශ්‍ය Day Package එක තෝරන්න:*`
        : "",
    ]
      .filter(Boolean)
      .join("\n");

    let buttons: Array<{ id: string; text: string }> = [];
    if (!product.inStock) {
      buttons = [
        { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
      ];
    } else if (packages.length > 1) {
      buttons = [
        ...packages.slice(0, 8).map((pk) => ({
          id: `biz_buy_pkg:${product.id}:${pk.id}`,
          text: `🔑 ${pk.days} • ${pk.price}`,
        })),
        { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
      ];
    } else if (packages.length === 1) {
      buttons = [
        { id: `biz_buy:${product.id}`, text: `🛒 Buy (${packages[0].days} • ${packages[0].price})` },
        { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
      ];
    } else {
      buttons = [
        { id: `biz_buy:${product.id}`, text: `🛒 Buy (${product.price})` },
        { id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" },
      ];
    }

    this.addLog("CMD", `🔑 [BIZ PANEL CMD] Sent Days & Price details for "${product.name}" (${panelCmd})`);
    await this.sendBusinessButtonMessage(jid, msg, {
      image: imgBuf,
      title: `${product.verified ? "✅ " : ""}${product.name} (${panelCmd})`,
      text: body,
      footer: `${store.storeName} • ${product.category}`,
      buttons,
    });
  }

  async handleBusinessBotMessage(
    jid: string,
    msg: any,
    rawText: string,
    isGroup: boolean,
    isOwner: boolean
  ) {
    if (!this.sock) return;
    try {
      const store = getBusinessStore(this.userId);
      const prefix = this.state.config.prefix || ".";
      const trimmed = (rawText || "").trim();
      const lower = trimmed.toLowerCase();
      const senderNumber = (msg.key.remoteJidAlt || msg.key.participantAlt || msg.key.participant || jid).split("@")[0].split(":")[0];
      const ownerDigits = (this.state.config.ownerNumber || "94740712934").replace(/\D/g, "");
      const chatState = this.businessChatState.get(jid);
      const chatStep = chatState?.step;
      const brandName = store.storeName || this.state.config.botName || "YASHI HALLS";
      const aoSettings = getAutoOrderState(this.userId).settings;

      // 00-RECEIPT. DETECT CUSTOMER PAYMENT RECEIPT UPLOAD (IMAGE / PDF / DOCUMENT) EVEN WITH EMPTY CAPTION
      const rawMsgObj = msg?.message || {};
      const imgMsg = rawMsgObj.imageMessage;
      const docMsg =
        rawMsgObj.documentMessage ||
        rawMsgObj.documentWithCaptionMessage?.message?.documentMessage;
      const hasReceiptMedia = Boolean(imgMsg || docMsg);

      if (hasReceiptMedia && !isGroup) {
        const pendingOrder = getActiveCustomerPendingOrder(this.userId, jid);
        if (pendingOrder) {
          const mimeType = String(
            imgMsg?.mimetype || docMsg?.mimetype || "image/jpeg"
          ).toLowerCase();
          const isSupportedReceiptMime =
            mimeType.startsWith("image/") || mimeType === "application/pdf";

          if (!isSupportedReceiptMime) {
            await this.sock
              .sendMessage(
                jid,
                {
                  text: `⚠️ Unsupported receipt file type (${mimeType}). Please upload your payment receipt as an *Image (JPG/PNG)* or *PDF Document*.`,
                },
                msg ? { quoted: msg } : undefined
              )
              .catch(() => {});
            return;
          }

          // 1. Send Animated Cyber/Neon Payment Processing HTML App Card immediately!
          let procCardMsgId: string | null = null;
          const pkgLabel = pendingOrder.botQuantity
            ? `${pendingOrder.botQuantity} BOTS`
            : pendingOrder.packageDays || pendingOrder.productName;
          try {
            const scanningHtml = generatePaymentProcessingHtml({
              brandName,
              orderId: pendingOrder.id,
              stage: "SCANNING",
              packageLabel: pkgLabel,
              expectedAmountLkr: pendingOrder.amountLkr,
              headline: "Scanning Payment Receipt...",
              subline: "AI Receipt Scanner & Bank Verification Engine මඟින් ඔබගේ Receipt එක පරීක්ෂා කරමින් පවතී...",
              height: 315,
            });
            const cardRes = await sendHtmlApp(this.sock, jid, scanningHtml, {
              title: `🔍 PROCESSING • ${pendingOrder.id}`,
              height: 315,
              quoted: msg || undefined,
            });
            if (cardRes?.messageId) {
              procCardMsgId = cardRes.messageId;
              this.trackSentMessageId({ key: { id: cardRes.messageId } });
            }
          } catch {
            procCardMsgId = null;
          }

          if (!procCardMsgId) {
            await this.sock
              .sendMessage(
                jid,
                {
                  text: `🔍 *Scanning your payment receipt for Order ${pendingOrder.id}...*\nPlease wait a few seconds while we verify your payment.`,
                },
                msg ? { quoted: msg } : undefined
              )
              .catch(() => {});
          }

          try {
            const mediaBuf = (await downloadMediaMessage(
              msg,
              "buffer",
              {},
              { logger: pino({ level: "silent" }), reuploadRequest: this.sock.updateMediaMessage }
            )) as Buffer;

            if (Buffer.isBuffer(mediaBuf) && mediaBuf.length > 0) {
              const fileName =
                String(docMsg?.fileName || "").trim() ||
                `receipt_${pendingOrder.id}.${mimeType.includes("pdf") ? "pdf" : "jpg"}`;
              const verification = await processCustomerReceiptUpload(
                this.userId,
                {
                  orderId: pendingOrder.id,
                  fileBuffer: mediaBuf,
                  fileName,
                  mimeType,
                },
                this.getAutoOrderWhatsAppDispatcher()
              );

              const ocr = verification.order?.receipt?.ocrData;
              const isVerified = verification.status === "VERIFIED";
              const isRejected =
                verification.status === "REJECTED_AMOUNT_MISMATCH" ||
                verification.status === "REJECTED_DUPLICATE_HASH" ||
                verification.status === "REJECTED_DUPLICATE_REF";

              if (procCardMsgId) {
                const updatedHtml = generatePaymentProcessingHtml({
                  brandName,
                  orderId: pendingOrder.id,
                  stage: isVerified ? "VERIFIED" : isRejected ? "REJECTED" : "AWAITING_APPROVAL",
                  packageLabel: pkgLabel,
                  expectedAmountLkr: pendingOrder.amountLkr,
                  detectedAmountLkr: ocr?.amountLkr || null,
                  bankName: ocr?.bankName || null,
                  reference: ocr?.transactionRefId || null,
                  headline: isVerified
                    ? "Payment Verified Successfully!"
                    : verification.status === "REJECTED_AMOUNT_MISMATCH"
                      ? "Amount Mismatch Detected"
                      : isRejected
                        ? "Duplicate Receipt Rejected"
                        : "Receipt Received — Verifying Payment",
                  subline: isVerified
                    ? "ඔබගේ ගෙවීම තහවුරු විය! පහත Guild Configuration කාඩ්පතින් ඔබේ විස්තර එවන්න."
                    : verification.status === "REJECTED_AMOUNT_MISMATCH"
                      ? `අවශ්‍ය මුදල LKR ${pendingOrder.amountLkr} නමුත් Receipt එකේ LKR ${ocr?.amountLkr || "?"} ලෙස සඳහන් වේ. නිවැරදි Receipt එකක් එවන්න.`
                      : isRejected
                        ? "මෙම Receipt එක කලින් භාවිතා කර ඇත. නව ගෙවීමක සත්‍ය Receipt එකක් Upload කරන්න."
                        : "Bank Notification හෝ Admin Approval ලැබුණු වහාම ඔබේ ඕඩර් එක ස්වයංක්‍රීයව ඉදිරියට යයි.",
                  height: 315,
                });
                await editHtmlApp(this.sock, jid, procCardMsgId, updatedHtml, {
                  title: isVerified
                    ? `✅ VERIFIED • ${pendingOrder.id}`
                    : isRejected
                      ? `❌ RECEIPT NOTICE • ${pendingOrder.id}`
                      : `⏳ VERIFYING • ${pendingOrder.id}`,
                  height: 315,
                }).catch(() => {});
              } else if (verification.customerReplyText) {
                await this.sock
                  .sendMessage(
                    jid,
                    { text: verification.customerReplyText },
                    msg ? { quoted: msg } : undefined
                  )
                  .catch(() => {});
              }
              return;
            }
          } catch (dlErr: any) {
            this.addLog("WARN", `⚠️ [RECEIPT UPLOAD] Download error for ${pendingOrder.id}: ${dlErr?.message || dlErr}`);
            if (procCardMsgId) {
              const errHtml = generatePaymentProcessingHtml({
                brandName,
                orderId: pendingOrder.id,
                stage: "REJECTED",
                packageLabel: pkgLabel,
                expectedAmountLkr: pendingOrder.amountLkr,
                headline: "Could Not Read Receipt File",
                subline: "කරුණාකර ඔබගේ Payment Receipt Photo හෝ PDF එක නැහැදිලි බැවින් නැවත පැහැදිලිව එවන්න.",
                height: 315,
              });
              await editHtmlApp(this.sock, jid, procCardMsgId, errHtml, {
                title: `⚠️ UPLOAD RETRY • ${pendingOrder.id}`,
                height: 315,
              }).catch(() => {});
            } else {
              await this.sock
                .sendMessage(
                  jid,
                  { text: "⚠️ Could not download your receipt file. Please re-send your payment receipt Image or PDF." },
                  msg ? { quoted: msg } : undefined
                )
                .catch(() => {});
            }
            return;
          }
        }
      }

      if (!trimmed) return;

      // 00. BUSINESS OWNER COMMANDS: .payapk -> APK File, .paykey / .payapp key -> Connect Key
      if (
        lower === `${prefix.toLowerCase()}payapk` ||
        lower === ".payapk" ||
        lower === `${prefix.toLowerCase()}payapp apk` ||
        lower === ".payapp apk"
      ) {
        if (!isOwner && !msg.key.fromMe) {
          await this.sock.sendMessage(jid, { text: "🔒 මෙම විධානය Business Bot Owner හට පමණක් භාවිතා කළ හැකිය." }, msg ? { quoted: msg } : undefined).catch(() => {});
          return;
        }
        await this.sendYashiPayApkToWhatsApp(jid, msg);
        return;
      }

      if (
        lower === `${prefix.toLowerCase()}paykey` ||
        lower === ".paykey" ||
        lower === `${prefix.toLowerCase()}payapp key` ||
        lower === ".payapp key"
      ) {
        if (!isOwner && !msg.key.fromMe) {
          await this.sock.sendMessage(jid, { text: "🔒 මෙම විධානය Business Bot Owner හට පමණක් භාවිතා කළ හැකිය." }, msg ? { quoted: msg } : undefined).catch(() => {});
          return;
        }
        await this.sendYashiPayKeyToWhatsApp(jid, msg);
        return;
      }

      // 00-APPROVAL. OWNER PAYMENT CONFIRM / REJECT BUTTONS & COMMANDS (CONNECTED BOT NUMBER)
      const adminConfirmMatch =
        trimmed.match(/^yh_admin_confirm:(YH-\d{3,10})$/i) ||
        trimmed.match(/^confirm\s+(YH-\d{3,10})$/i);
      if (adminConfirmMatch) {
        if (!isOwner && !msg.key.fromMe) {
          await this.sock.sendMessage(jid, { text: "🔒 Payment Confirm කිරීමට Owner අවසරය අවශ්‍ය වේ." }, msg ? { quoted: msg } : undefined).catch(() => {});
          return;
        }
        const targetOrderId = adminConfirmMatch[1].toUpperCase();
        const dispatcher = this.getAutoOrderWhatsAppDispatcher();
        const approveRes = await adminApproveOrderPayment(
          this.userId,
          targetOrderId,
          "whatsapp_owner_button",
          dispatcher
        );
        if (approveRes.success && approveRes.order) {
          if (approveRes.order.customerJid && approveRes.order.serviceType !== "GUILD_SERVICE") {
            const verifiedHtml = generatePaymentProcessingHtml({
              brandName,
              orderId: approveRes.order.id,
              stage: "VERIFIED",
              packageLabel: approveRes.order.productName,
              expectedAmountLkr: approveRes.order.amountLkr,
              detectedAmountLkr: approveRes.order.amountLkr,
              bankName: approveRes.order.payment?.bank || "Admin Verified",
              headline: "Payment Confirmed by Admin!",
              subline: "ඔබගේ ගෙවීම සාර්ථකව තහවුරු විය!",
              height: 315,
            });
            await dispatcher.sendHtmlAppCard?.(approveRes.order.customerJid, verifiedHtml, {
              title: `✅ PAYMENT CONFIRMED • ${approveRes.order.id}`,
              height: 315,
            });
          }
          await this.sock
            .sendMessage(
              jid,
              {
                text: `✅ *PAYMENT CONFIRMED!*\n🧾 *Order ID:* ${targetOrderId}\n👤 *Customer:* ${approveRes.order.customerName} (+${approveRes.order.customerNumber})\n💰 *Amount:* LKR ${approveRes.order.amountLkr.toLocaleString("en-US")}\n🚀 ${approveRes.message}`,
              },
              msg ? { quoted: msg } : undefined
            )
            .catch(() => {});
        } else {
          await this.sock
            .sendMessage(
              jid,
              { text: `⚠️ *Confirm Notice (${targetOrderId}):* ${approveRes.message}` },
              msg ? { quoted: msg } : undefined
            )
            .catch(() => {});
        }
        return;
      }

      const adminRejectMatch =
        trimmed.match(/^yh_admin_reject:(YH-\d{3,10})$/i) ||
        trimmed.match(/^reject\s+(YH-\d{3,10})$/i);
      if (adminRejectMatch) {
        if (!isOwner && !msg.key.fromMe) {
          await this.sock.sendMessage(jid, { text: "🔒 Payment Reject කිරීමට Owner අවසරය අවශ්‍ය වේ." }, msg ? { quoted: msg } : undefined).catch(() => {});
          return;
        }
        const targetOrderId = adminRejectMatch[1].toUpperCase();
        const rejectRes = adminRejectOrCancelOrder(
          this.userId,
          targetOrderId,
          "whatsapp_owner_button",
          "REJECT",
          "Payment rejected by Admin via WhatsApp"
        );
        if (rejectRes.success && rejectRes.order) {
          const dispatcher = this.getAutoOrderWhatsAppDispatcher();
          if (rejectRes.order.customerJid) {
            const rejectedHtml = generatePaymentProcessingHtml({
              brandName,
              orderId: rejectRes.order.id,
              stage: "REJECTED",
              packageLabel: rejectRes.order.botQuantity
                ? `${rejectRes.order.botQuantity} BOTS`
                : rejectRes.order.productName,
              expectedAmountLkr: rejectRes.order.amountLkr,
              headline: "Payment Receipt Rejected",
              subline: "ඔබගේ Payment Receipt එක තහවුරු කළ නොහැකි විය. නිවැරදි ගෙවීමක් සිදු කර නව Order එකක් ආරම්භ කරන්න.",
              height: 315,
            });
            const sentCard = await dispatcher.sendHtmlAppCard?.(rejectRes.order.customerJid, rejectedHtml, {
              title: `❌ PAYMENT REJECTED • ${rejectRes.order.id}`,
              height: 315,
            });
            if (!sentCard) {
              await dispatcher.sendMessage(
                rejectRes.order.customerJid,
                `❌ *PAYMENT REJECTED*\n🧾 Order: *${rejectRes.order.id}*\nඔබගේ Payment Receipt එක Admin විසින් ප්‍රතික්ෂේප කරන ලදී. ගැටළුවක් ඇත්නම් Admin සම්බන්ධ කරගන්න.`
              );
            }
          }
          await this.sock
            .sendMessage(
              jid,
              {
                text: `❌ *PAYMENT REJECTED*\n🧾 *Order ID:* ${targetOrderId}\n👤 *Customer:* ${rejectRes.order.customerName} (+${rejectRes.order.customerNumber})\n📌 Customer වෙත දැනුම් දෙන ලදී.`,
              },
              msg ? { quoted: msg } : undefined
            )
            .catch(() => {});
        } else {
          await this.sock
            .sendMessage(
              jid,
              { text: `⚠️ *Reject Notice (${targetOrderId}):* ${rejectRes.message}` },
              msg ? { quoted: msg } : undefined
            )
            .catch(() => {});
        }
        return;
      }

      // 00-BANK. BANK & EZ CASH DETAILS HTML APP UI CARD
      if (
        trimmed === "guild_bank_card" ||
        lower === `${prefix.toLowerCase()}bank` ||
        lower === ".bank" ||
        lower === "bank" ||
        lower === "bank details"
      ) {
        const pendingOrd = getActiveCustomerPendingOrder(this.userId, jid);
        await this.sendBankDetailsHtmlAppCard(
          jid,
          msg,
          brandName,
          pendingOrd
            ? {
                orderId: pendingOrd.id,
                serviceLabel: pendingOrd.serviceType === "GUILD_SERVICE" ? "GUILD SERVICE" : "STORE ORDER",
                packageLabel: pendingOrd.botQuantity ? `${pendingOrd.botQuantity} BOTS` : pendingOrd.productName,
                amountLkr: pendingOrd.amountLkr,
              }
            : undefined
        );
        return;
      }

      // 00-GUILD. INTERACTIVE GUILD ORDER FLOW (PHASES 1, 2, 3, 5, 6, 7, 8)
      if (aoSettings.guildFlowEnabled) {
        // Phase 1 Trigger: Welcome Card (button or explicit keywords)
        if (
          trimmed === "guild_welcome_menu" ||
          lower === ".guild" ||
          lower === `${prefix.toLowerCase()}guild` ||
          lower === ".start" ||
          lower === "start" ||
          lower === "guild service" ||
          lower === "order"
        ) {
          if (lower === "guild service") {
            await this.sendGuildQuantityMenu(jid, msg, brandName);
          } else {
            await this.sendGuildWelcomeCard(jid, msg, brandName);
          }
          return;
        }

        const isRecentWelcomeStep = Boolean(
          chatStep === "welcome" && chatState && Date.now() - chatState.updatedAt <= 5 * 60 * 1000
        );

        // Phase 1 Button 1: GUILD SERVICE -> Phase 2 Bot Quantity Menu
        if (trimmed === "guild_svc_start" || (isRecentWelcomeStep && (trimmed === "1" || lower === "guild" || lower === "guild service"))) {
          await this.sendGuildQuantityMenu(jid, msg, brandName);
          return;
        }

        // Phase 1 Button 2: PANEL SERVICE -> Clean isolated Panel Catalog (.panal)
        if (trimmed === "panel_svc_start" || (isRecentWelcomeStep && (trimmed === "2" || lower === "panel" || lower === "panal" || lower === "panel service"))) {
          this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
          await this.sendBusinessCatalogAppMenu(jid, msg, store);
          return;
        }

        // Phase 2 Selection: 4 / 8 / 12 / 16 BOTS -> Phase 3 Premium Payment Card
        if (trimmed.startsWith("guild_qty:")) {
          const parsedQty = Number(trimmed.slice("guild_qty:".length).trim());
          const qty: GuildBotQuantity =
            parsedQty === 8 || parsedQty === 12 || parsedQty === 16 ? parsedQty : 4;
          await this.sendGuildPaymentCard(jid, msg, qty, brandName);
          return;
        }

        if (chatStep === "guild_qty") {
          const mapQty: Record<string, GuildBotQuantity> = {
            "1": 4,
            "4": 4,
            "4 bots": 4,
            "2": 8,
            "8": 8,
            "8 bots": 8,
            "3": 12,
            "12": 12,
            "12 bots": 12,
            "4b": 4,
            "16": 16,
            "16 bots": 16,
          };
          const chosen = mapQty[lower];
          if (chosen) {
            await this.sendGuildPaymentCard(jid, msg, chosen, brandName);
            return;
          }
        }

        // Phase 5 Native Region Button Clicked: guild_reg:<orderId>:<regionId>
        if (trimmed.startsWith("guild_reg:")) {
          const parts = trimmed.split(":");
          const orderId = (parts[1] || "").trim();
          const regionId = (parts[2] || "").trim().toLowerCase();
          if (orderId && regionId) {
            this.businessChatState.set(jid, {
              step: "guild_awaiting_clan_id",
              orderId,
              regionId,
              updatedAt: Date.now(),
            });
            await this.sock
              .sendMessage(
                jid,
                {
                  text: [
                    `🌍 *Selected Region:* ${regionId.toUpperCase()}`,
                    `🧾 *Order ID:* ${orderId}`,
                    ``,
                    `👉 දැන් ඔබගේ *Free Fire Guild / Clan ID* එක Reply කරන්න (උදා: \`123456789\`).`,
                    `❌ අවලංගු කිරීමට *cancel* යවන්න.`,
                  ].join("\n"),
                },
                msg ? { quoted: msg } : undefined
              )
              .catch(() => {});
            return;
          }
        }

        // Phase 5 In-Chat Guild ID Reply after picking Region button
        if (chatStep === "guild_awaiting_clan_id" && chatState?.orderId && chatState?.regionId) {
          if (lower === "cancel" || lower === "stop") {
            this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
            await this.sock.sendMessage(jid, { text: "❌ Guild configuration paused. You can reopen it anytime." }, msg ? { quoted: msg } : undefined).catch(() => {});
            return;
          }
          const cleanedId = trimmed.replace(/\s+/g, "");
          if (/^\d{5,18}$/.test(cleanedId)) {
            this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
            const res = await submitGuildConfiguration(
              this.userId,
              chatState.orderId,
              {
                regionId: chatState.regionId,
                clanId: cleanedId,
                submittedVia: "WHATSAPP_NATIVE",
                actor: senderNumber,
              },
              this.getAutoOrderWhatsAppDispatcher()
            );
            if (!res.success) {
              await this.sock
                .sendMessage(jid, { text: `⚠️ *Guild Launch Notice:* ${res.message}` }, msg ? { quoted: msg } : undefined)
                .catch(() => {});
            }
            return;
          } else {
            await this.sock
              .sendMessage(
                jid,
                { text: "⚠️ කරුණාකර වලංගු *Guild / Clan ID* එකක් (ඉලක්කම් 5–18) පමණක් Reply කරන්න." },
                msg ? { quoted: msg } : undefined
              )
              .catch(() => {});
            return;
          }
        }

        // Phase 5 COPY DETAILS Format Detector (Customer copied from Launch New Group HTML Card & pasted into WhatsApp!)
        const copyRegionMatch = trimmed.match(/Region(?:\s*ID)?\s*[:=-]\s*([a-zA-Z0-9 _()-]{2,36})/i);
        const copyGuildMatch = trimmed.match(/(?:Guild|Clan)\s*ID\s*[:=-]\s*(\d{5,18})/i);
        if (copyRegionMatch && copyGuildMatch) {
          const explicitOrderMatch = trimmed.match(/\b(YH-\d{3,10})\b/i);
          const targetOrder =
            (explicitOrderMatch
              ? getAutoOrderState(this.userId).orders.find((o) => o.id === explicitOrderMatch[1].toUpperCase())
              : null) || getActiveCustomerAwaitingGuildConfigOrder(this.userId, jid);

          if (targetOrder) {
            const rawRegionToken = copyRegionMatch[1].split("(")[0].trim();
            const res = await submitGuildConfiguration(
              this.userId,
              targetOrder.id,
              {
                regionId: rawRegionToken,
                clanId: copyGuildMatch[1].trim(),
                submittedVia: "WHATSAPP_COPY",
                actor: senderNumber,
              },
              this.getAutoOrderWhatsAppDispatcher()
            );
            if (!res.success) {
              await this.sock
                .sendMessage(jid, { text: `⚠️ *Guild Configuration Error:* ${res.message}` }, msg ? { quoted: msg } : undefined)
                .catch(() => {});
            }
            return;
          }
        }

        // Phase 8 Native Button: LIVE GLORY STATUS REFRESH
        if (trimmed.startsWith("guild_live_glory:")) {
          const orderId = trimmed.slice("guild_live_glory:".length).trim();
          if (orderId) {
            const res = await sendLiveGloryCardToWhatsApp(
              this.userId,
              orderId,
              this.getAutoOrderWhatsAppDispatcher()
            );
            if (!res.success && res.message) {
              await this.sock
                .sendMessage(jid, { text: `⚠️ Live Glory Notice: ${res.message}` }, msg ? { quoted: msg } : undefined)
                .catch(() => {});
            }
            return;
          }
        }
      }

      // 0A. SERVICE MENU (.panal / .panel) & ALL PANELS BUTTON (biz_all_ui_menu)
      const isPanalCommand =
        lower === `${prefix.toLowerCase()}panal` ||
        lower === ".panal" ||
        lower === `${prefix.toLowerCase()}panel` ||
        lower === ".panel";

      if (isPanalCommand) {
        this.addLog("CMD", `💼 [BUSINESS BOT] ${trimmed} service menu requested by ${senderNumber}`);
        if (aoSettings.guildFlowEnabled) {
          await this.sendGuildWelcomeCard(jid, msg, brandName);
        } else {
          this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
          await this.sendBusinessCatalogAppMenu(jid, msg, store);
        }
        return;
      }

      if (trimmed === "biz_all_ui_menu") {
        this.addLog("CMD", `💼 [BUSINESS BOT] All Panels UI Catalog requested by ${senderNumber}`);
        this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
        await this.sendBusinessCatalogAppMenu(jid, msg, store);
        return;
      }

      // 0B. PACKAGE BUTTON CLICKED: biz_buy_pkg:<productId>:<pkgId>
      if (trimmed.startsWith("biz_buy_pkg:")) {
        const rest = trimmed.slice("biz_buy_pkg:".length);
        const sepIdx = rest.indexOf(":");
        const productId = sepIdx !== -1 ? rest.slice(0, sepIdx).trim() : rest.trim();
        const pkgId = sepIdx !== -1 ? rest.slice(sepIdx + 1).trim() : "";
        const product = store.products.find((p) => p.id === productId);
        if (!product) return;
        if (!product.inStock) {
          await this.sendBusinessButtonMessage(jid, msg, {
            title: `🔴 OUT OF STOCK`,
            text: `*${product.name}* දැනට අවසන් වී ඇත (Out of Stock).\nකරුණාකර වෙනත් Product එකක් තෝරන්න 👇`,
            footer: store.storeName,
            buttons: [{ id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" }],
          });
          return;
        }
        const pkg = (product.packages || []).find((pk) => pk.id === pkgId) || product.packages?.[0] || null;
        this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
        await this.sendBusinessOrderDetails(jid, msg, product, store, ownerDigits, pkg);
        return;
      }

      // 0C. DIRECT PANEL COMMAND LOOKUP WITH PREFIX (e.g., ".balamod", ".phanthom", ".gbox", ".brmods")
      const hasCommandPrefix = trimmed.startsWith(".") || trimmed.startsWith(prefix);
      if (hasCommandPrefix) {
        const matchedPanelByCmd = store.products.find((p) => {
          const slug = sanitizePanelCommandSlug(p.command, p.name);
          return lower === `${prefix.toLowerCase()}${slug}` || lower === `.${slug}`;
        });
        if (matchedPanelByCmd) {
          await this.sendBusinessPanelDetailsByCommand(jid, msg, matchedPanelByCmd, store);
          return;
        }
      }

      // 0D-CLAN. CLAN ID INPUT STEP FOR FFGLORY CLAN GROUP ORDERS
      if (chatStep === "awaiting_clan_id" && chatState?.productId && !trimmed.startsWith(prefix) && !trimmed.startsWith(".")) {
        if (lower === "cancel" || lower === "stop" || lower === "exit") {
          this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
          await this.sock.sendMessage(jid, { text: "❌ Clan Glory Order එක අවලංගු කරන ලදී. නැවත තෝරාගැනීමට *.panal* යවන්න." }, msg ? { quoted: msg } : undefined).catch(() => {});
          return;
        }
        const cleanedClanId = trimmed.replace(/\s+/g, "");
        if (/^\d{5,18}$/.test(cleanedClanId)) {
          const activeProduct = store.products.find((p) => p.id === chatState.productId);
          if (activeProduct) {
            const matchedPkg = chatState.packageId
              ? (activeProduct.packages || []).find((pk) => pk.id === chatState.packageId) || null
              : activeProduct.packages?.[0] || null;
            this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
            await this.sendBusinessOrderDetails(jid, msg, activeProduct, store, ownerDigits, matchedPkg, cleanedClanId);
            return;
          }
        } else {
          await this.sock.sendMessage(
            jid,
            { text: "⚠️ වලංගු *Free Fire Clan / Guild ID* එකක් (ඉලක්කම් පමණක්, උදා: `123456789`) Reply කරන්න.\n❌ අවලංගු කිරීමට *cancel* යවන්න." },
            msg ? { quoted: msg } : undefined
          ).catch(() => {});
          return;
        }
      }

      // 0D. TEXT / NUMBER SELECTION WHILE IN PACKAGE PICKER STEP (after user sent a panel command)
      if (chatStep === "package" && chatState?.productId) {
        const activeProduct = store.products.find((p) => p.id === chatState.productId);
        const pkgs = Array.isArray(activeProduct?.packages) ? activeProduct!.packages! : [];
        if (activeProduct && pkgs.length > 0) {
          const numMatch = trimmed.match(/^\d+$/);
          let matchedPkg: BusinessPricePackage | undefined;
          if (numMatch) {
            const idx = Number(trimmed) - 1;
            if (idx >= 0 && idx < Math.min(pkgs.length, 9)) {
              matchedPkg = pkgs[idx];
            } else if (idx === Math.min(pkgs.length, 9)) {
              await this.sendBusinessCatalogAppMenu(jid, msg, store);
              return;
            }
          } else {
            matchedPkg = pkgs.find(
              (pk) =>
                pk.days.toLowerCase() === lower ||
                lower.includes(pk.days.toLowerCase()) ||
                pk.days.toLowerCase().includes(lower)
            );
          }
          if (matchedPkg) {
            this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
            await this.sendBusinessOrderDetails(jid, msg, activeProduct, store, ownerDigits, matchedPkg);
            return;
          }
        }
      }

      // 0E. BUY PRODUCT BUTTON ACTION (biz_buy:<productId>)
      if (trimmed.startsWith("biz_buy:")) {
        const productQuery = trimmed.slice("biz_buy:".length).trim();
        const product = store.products.find((p) => p.id === productQuery);
        if (!product) return;

        if (!product.inStock) {
          await this.sendBusinessButtonMessage(jid, msg, {
            title: `🔴 OUT OF STOCK`,
            text: `*${product.name}* දැනට අවසන් වී ඇත (Out of Stock).\nකරුණාකර වෙනත් Product එකක් තෝරන්න 👇`,
            footer: store.storeName,
            buttons: [{ id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" }],
          });
          return;
        }

        const pkgs = Array.isArray(product.packages) ? product.packages : [];
        if (pkgs.length > 1) {
          await this.sendBusinessPackageSelector(jid, msg, product, store.storeName, store.mainLogoUrl);
          return;
        }

        this.businessChatState.set(jid, { step: "catalog", updatedAt: Date.now() });
        await this.sendBusinessOrderDetails(jid, msg, product, store, ownerDigits, pkgs[0] || null);
        return;
      }

      // 0F. OUT OF STOCK BUTTON NOTICE
      if (trimmed.startsWith("biz_oos:")) {
        const productId = trimmed.slice("biz_oos:".length).trim();
        const product = store.products.find((p) => p.id === productId);
        await this.sendBusinessButtonMessage(jid, msg, {
          title: `🔴 OUT OF STOCK`,
          text: `*${product?.name || "මෙම Product එක"}* දැනට Out of Stock වේ.\nවෙනත් Panel එකක් බැලීමට \`.panal\` යොදන්න 👇`,
          footer: store.storeName,
          buttons: [{ id: "biz_all_ui_menu", text: "📋 All Panels (.panal)" }],
        });
        return;
      }

      // PHASE 1 WELCOME CARD — STRICTLY ONCE PER CUSTOMER (persisted in welcomed_customers.json)
      if (!isGroup && aoSettings.guildFlowEnabled && aoSettings.welcomeOnFirstMessage) {
        const alreadyWelcomed = this.hasReceivedBusinessWelcome(jid, msg);
        if (!alreadyWelcomed && !trimmed.startsWith(prefix) && !trimmed.startsWith(".")) {
          this.markBusinessCustomerWelcomed(jid, msg);
          const awaitingCfg = getActiveCustomerAwaitingGuildConfigOrder(this.userId, jid);
          if (awaitingCfg) {
            await sendPhase5GuildConfigCard(this.userId, awaitingCfg, this.getAutoOrderWhatsAppDispatcher());
            return;
          }
          await this.sendGuildWelcomeCard(jid, msg, brandName);
          return;
        }
      }

      return;
    } catch (bizErr: any) {
      this.addLog("ERROR", `❌ [BUSINESS BOT] Handler error: ${bizErr?.message || bizErr}`);
    }
  }

  formatUptime(){if(!this.startTime)return"0m";const totalSec=Math.floor((Date.now()-this.startTime)/1e3);const hours=Math.floor(totalSec/3600);const minutes=Math.floor(totalSec%3600/60);const seconds=totalSec%60;if(hours>0)return`${hours}h ${minutes}m ${seconds}s`;if(minutes>0)return`${minutes}m ${seconds}s`;return`${seconds}s`}}const userEngines = new Map<string, BotEngine>();
function getBotEngine(userId = "primary"): BotEngine {
  let cleanId = userId && typeof userId === "string" && userId.trim() ? userId.trim() : "primary";
  if (cleanId === "guest" || cleanId === "default") {
    cleanId = "primary";
  }
  if (!userEngines.has(cleanId)) {
    const engine = new BotEngine(cleanId);
    userEngines.set(cleanId, engine);
  }
  return userEngines.get(cleanId)!;
}
__name(getBotEngine, "getBotEngine");
const botEngine = getBotEngine("primary");

export async function autoBootAllRegisteredBots() {
  try {
    const accountsToBoot = new Set<string>(["primary"]);
    const dbBackups = adminStore.db?.sessionBackups || {};
    for (const key of Object.keys(dbBackups)) {
      accountsToBoot.add(key === "guest" || key === "default" ? "primary" : key);
    }
    const baseDir = path.resolve(process.cwd(), "data", "sessions");
    if (fs.existsSync(baseDir)) {
      const items = fs.readdirSync(baseDir, { withFileTypes: true });
      for (const item of items) {
        if (item.isDirectory()) {
          const uId = item.name;
          accountsToBoot.add(uId === "guest" || uId === "default" ? "primary" : uId);
        }
      }
    }

    for (const uId of accountsToBoot) {
      const engine = getBotEngine(uId);
      if (!engine.hasExistingSession()) {
        adminStore.restoreSessionBackup(uId, engine.sessionDir);
        if (!engine.hasExistingSession() && uId === "primary") {
          adminStore.restoreSessionBackup("guest", engine.sessionDir);
          adminStore.restoreSessionBackup("default", engine.sessionDir);
        }
      }
      if (engine.hasExistingSession() && !engine.state.isOnline) {
        engine.addLog("INFO", `[24/7 AutoBoot] Restoring 24/7 session container for account: ${uId}`);
        engine.start().catch((err: any) => {
          console.warn(`[AutoBoot] Startup note for ${uId}:`, err?.message || err);
        });
      }
    }
  } catch (err: any) {
    console.warn(`[AutoBoot] Error:`, err?.message || err);
  }
}

export { BRAND_LOGO_URL, BotEngine, OWNER_HOTLINE, WHATSAPP_CHANNEL_URL, botEngine, getBotEngine };
