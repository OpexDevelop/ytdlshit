
import express from 'express';
import pkg from 'grammy';
const { Bot, GrammyError, HttpError, InlineKeyboard, InputFile, InputMediaBuilder } = pkg;
import { run } from "@grammyjs/runner";
import { limit } from '@grammyjs/ratelimiter';
import { apiThrottler } from '@grammyjs/transformer-throttler';
import { autoRetry } from '@grammyjs/auto-retry';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
// import { downloadYouTubeVideo } from '@opexdevelop/cnvmp3-dl';
import { downloadYouTubeVideo } from './ytdl.js';
import { getVideo, searchVideos } from 'opex-yt-info';
import { getYouTubeVideoId } from 'opex-yt-id';
import { downloadSpotifyTrack, getTrackMetadata } from '@opexdevelop/spotify-dl';
import { getTikTokInfo, downloadTikTok } from '@opexdevelop/tiktok-dl';
import { initializeDatabase, User, Message, Op, sequelize, upsertUser, recordMessage } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
let config;
try {
    const configPath = path.resolve(__dirname, 'env.json');
    const configFile = await fs.readFile(configPath, 'utf-8');
    config = JSON.parse(configFile);
} catch (err) {
    console.error("CRITICAL: Error reading or parsing env.json:", err);
    console.error("Please ensure env.json exists in the project root and contains all required fields (BOT_TOKEN, DB_*, CA_CERT_PATH, BOT_ADMIN_ID).");
    process.exit(1);
}

const BOT_TOKEN = config.BOT_TOKEN;
const EXPRESS_PORT = 30077;
const BOT_ADMIN_ID = parseInt(config.BOT_ADMIN_ID, 10);
const TARGET_CHANNEL_ID = -1002505399520;
const CACHE_FILE_PATH = path.resolve(__dirname, 'file_id_cache.json');
const USER_LANG_CACHE_PATH = path.resolve(__dirname, 'user_languages.json');
const INLINE_SEARCH_LIMIT = 20;

const requiredConfigKeys = ['BOT_TOKEN', 'DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'CA_CERT_PATH', 'BOT_ADMIN_ID'];
for (const key of requiredConfigKeys) {
    if (!config[key]) {
        console.error(`CRITICAL: Configuration key "${key}" is missing in env.json!`);
        process.exit(1);
    }
}
if (isNaN(BOT_ADMIN_ID)) {
     console.error(`CRITICAL: BOT_ADMIN_ID ("${config.BOT_ADMIN_ID}") in env.json is not a valid number!`);
     process.exit(1);
}
if (!TARGET_CHANNEL_ID) {
    console.error(`CRITICAL: TARGET_CHANNEL_ID is not set! Ensure it's a valid chat ID (usually negative for channels/supergroups).`);
    process.exit(1);
}




const bot = new Bot(BOT_TOKEN, {
  client: { apiRoot: "http://localhost:30010" },
});
let botInfo;

let userLanguages = {};
const defaultLocale = "en";

let fileIdCache = {};

async function loadLangCache() {
    try {
        const data = await fs.readFile(USER_LANG_CACHE_PATH, 'utf-8');
        userLanguages = JSON.parse(data);
        console.log(`[Lang Cache] Loaded ${Object.keys(userLanguages).length} entries from ${USER_LANG_CACHE_PATH}`);
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log(`[Lang Cache] Cache file ${USER_LANG_CACHE_PATH} not found. Starting with empty cache.`);
            userLanguages = {};
        } else {
            console.error(`[Lang Cache] Error loading cache file ${USER_LANG_CACHE_PATH}:`, error);
            userLanguages = {};
        }
    }
}

async function saveLangCache() {
    try {
        await fs.writeFile(USER_LANG_CACHE_PATH, JSON.stringify(userLanguages, null, 2));
    } catch (error) {
        console.error(`[Lang Cache] Error saving cache file ${USER_LANG_CACHE_PATH}:`, error);
    }
}

async function loadCache() {
    try {
        const data = await fs.readFile(CACHE_FILE_PATH, 'utf-8');
        fileIdCache = JSON.parse(data);
        console.log(`[File Cache] Loaded ${Object.keys(fileIdCache).length} entries from ${CACHE_FILE_PATH}`);
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log(`[File Cache] Cache file ${CACHE_FILE_PATH} not found. Starting with empty cache.`);
            fileIdCache = {};
        } else {
            console.error(`[File Cache] Error loading cache file ${CACHE_FILE_PATH}:`, error);
            fileIdCache = {};
        }
    }
}

async function saveCache() {
    try {
        await fs.writeFile(CACHE_FILE_PATH, JSON.stringify(fileIdCache, null, 2));
    } catch (error) {
        console.error(`[File Cache] Error saving cache file ${CACHE_FILE_PATH}:`, error);
    }
}

const translations = {
    en: {
        welcome: "Welcome! Send me a YouTube video link (e.g., <code>https://www.youtube.com/watch?v=...</code>), Shorts, a Spotify track link (<code>https://open.spotify.com/track/...</code>), or a TikTok video link (<code>https://www.tiktok.com/...</code>) to download.\n\nOr type @{botUsername} ‹search query or link› in any chat.",
        language_select: "Please choose your language:",
        choose_format: "Choose the desired format:",
        choose_format_tiktok: "Choose format for TikTok video:",
        choose_quality_audio: "Choose audio quality (bitrate):",
        choose_quality_video: "Choose video quality (resolution):",
        requesting_download: "Requesting download/conversion, this may take some time...",
        sending_file: "Preparing the file for sending...",
        processing_detailed: "Processing: {format} ({quality})...",
        processing_spotify: "Processing Spotify track...",
        processing_tiktok: "Processing TikTok video...",
        download_ready: "✅ Here is your file!",
        action_cancelled: "Action cancelled.",
        error_select_language: "Please use /start to select a language first.",
        invalid_url: "The link you provided is not a valid YouTube, Spotify, or TikTok link. Please try again.",
        invalid_spotify_url: "Invalid Spotify track URL. Please use a link like: <code>https://open.spotify.com/track/...</code>",
        invalid_tiktok_url: "Invalid TikTok video URL. Please use a link like: <code>https://www.tiktok.com/...</code> or <code>https://vm.tiktok.com/...</code>",
        error_telegram_size: "Unfortunately, the bot can currently only send files smaller than 50MB.",
        length_limit_error: "Unfortunately, this video is too long to process. Please try a shorter video. (From API)",
        api_error: "An error occurred while processing your request via the download service: {error}",
        api_error_fetch: "Failed to connect to the download service. Please try again later. {error}",
        spotify_api_error: "An error occurred processing the Spotify link: {error}",
        spotify_metadata_failed: "Failed to get Spotify track details. Please check the link.",
        spotify_download_failed: "Failed to download the Spotify track: {error}",
        tiktok_api_error: "An error occurred processing the TikTok link: {error}",
        tiktok_metadata_failed: "Failed to get TikTok video details. Please check the link or try again.",
        tiktok_download_failed: "Failed to download the TikTok content: {error}",
        general_error: "An unexpected error occurred. Please try again. {error}",
        error_unexpected_action: "Unexpected action. Please start over by sending a link or searching.",
        error_occurred_try_again: "An unexpected error occurred. Please try again, starting with the /start command.",
        error_file_too_large: "The file ({size}) exceeds the bot's current limit for this type ({limit}). Download cancelled.",
        button_mp3: "MP3 (Audio)",
        button_mp4: "MP4 (Video)",
        button_cancel: "Cancel",
        '96kbps': "96 kbps", '128kbps': "128 kbps", '256kbps': "256 kbps", '320kbps': "320 kbps",
        '360p': "360p", '480p': "480p", '720p': "720p", '1080p': "1080p",
        processing: "Processing your request...",
        inline_processing: "⏳ Processing...",
        inline_result_title: "{format} - {quality}",
        inline_result_title_tiktok: "{format}",
        inline_invalid_url_prompt: "Invalid YouTube, Spotify, or TikTok URL",
        inline_description_direct: "Click to download {title} as {format} ({quality})",
        inline_description_spotify: "Click to download Spotify track: {title} by {artist}",
        inline_description_tiktok: "Click to download TikTok video as {format}",
        inline_edit_failed: "❌ Failed to load media.",
        inline_cache_upload_failed: "❌ Failed to prepare media. Try again.",
        inline_error_general: "❌ Error processing request.",
        error_fetching_title: "Error fetching video details.",
        fallback_video_title: "Video",
        fallback_track_title: "Track",
        fallback_tiktok_title: "TikTok Video",
        inline_search_result_title: "{title}",
        inline_search_result_description: "{author} • {views} views • {duration}",
        inline_search_no_results: "No videos found for '{query}'",
        inline_search_prompt: "Enter a search query, YouTube, Spotify, or TikTok link...",
        inline_search_select_final: "Video: {title}\nSelect format & quality 👇",
        inline_search_choose_format: "Select Format",
        inline_search_error: "❌ Search failed. Try again.",
        inline_format_selection: "Video: {title}\nChoose quality for {format} 👇",
        inline_processing_final: "Video: {title}\nProcessing {format} ({quality})...",
        inline_processing_spotify: "Track: {title}\nProcessing Spotify download...",
        inline_processing_tiktok: "TikTok: {title}\nProcessing {format} download...",
    },
    ru: {
        welcome: "Добро пожаловать! Отправьте мне ссылку на YouTube видео (<code>https://www.youtube.com/watch?v=...</code>), Shorts, трек Spotify (<code>https://open.spotify.com/track/...</code>) или видео TikTok (<code>https://www.tiktok.com/...</code>), чтобы скачать.\n\nИли введите @{botUsername} ‹запрос или ссылку› в любом чате.",
        language_select: "Пожалуйста, выберите ваш язык:",
        choose_format: "Выберите желаемый формат:",
        choose_format_tiktok: "Выберите формат для видео TikTok:",
        choose_quality_audio: "Выберите качество аудио (битрейт):",
        choose_quality_video: "Выберите качество видео (разрешение):",
        requesting_download: "Запрашиваю загрузку/конвертацию, это может занять некоторое время...",
        sending_file: "Файл готовится к отправке...",
        processing_detailed: "Обработка: {format} ({quality})...",
        processing_spotify: "Обработка трека Spotify...",
        processing_tiktok: "Обработка видео TikTok...",
        download_ready: "✅ Вот ваш файл!",
        action_cancelled: "Действие отменено.",
        error_select_language: "Пожалуйста, используйте /start, чтобы сначала выбрать язык.",
        invalid_url: "Предоставленная вами ссылка не является действительной ссылкой на YouTube, Spotify или TikTok. Пожалуйста, попробуйте еще раз.",
        invalid_spotify_url: "Неверная ссылка на трек Spotify. Используйте ссылку вида: <code>https://open.spotify.com/track/...</code>",
        invalid_tiktok_url: "Неверная ссылка на видео TikTok. Используйте ссылку вида: <code>https://www.tiktok.com/...</code> или <code>https://vm.tiktok.com/...</code>",
        error_telegram_size: "К сожалению, на данный момент бот может отправлять только файлы размером менее 50 МБ.",
        length_limit_error: "К сожалению, это видео слишком длинное для обработки. Пожалуйста, попробуйте видео покороче. (От API)",
        api_error: "Произошла ошибка при обработке вашего запроса через сервис загрузки: {error}",
        api_error_fetch: "Не удалось подключиться к сервису загрузки. Пожалуйста, попробуйте позже. {error}",
        spotify_api_error: "Произошла ошибка при обработке ссылки Spotify: {error}",
        spotify_metadata_failed: "Не удалось получить данные трека Spotify. Проверьте ссылку.",
        spotify_download_failed: "Не удалось загрузить трек Spotify: {error}",
        tiktok_api_error: "Произошла ошибка при обработке ссылки TikTok: {error}",
        tiktok_metadata_failed: "Не удалось получить данные видео TikTok. Проверьте ссылку или попробуйте позже.",
        tiktok_download_failed: "Не удалось загрузить контент TikTok: {error}",
        general_error: "Произошла непредвиденная ошибка. Пожалуйста, попробуйте снова. {error}",
        error_unexpected_action: "Неожиданное действие. Пожалуйста, начните заново, отправив ссылку или выполнив поиск.",
        error_occurred_try_again: "Произошла непредвиденная ошибка. Пожалуйста, попробуйте снова, начав с команды /start.",
        error_file_too_large: "Файл ({size}) превышает текущий лимит бота для этого типа ({limit}). Загрузка отменена.",
        button_mp3: "MP3 (Аудио)",
        button_mp4: "MP4 (Видео)",
        button_cancel: "Отмена",
        '96kbps': "96 кбит/с", '128kbps': "128 кбит/с", '256kbps': "256 кбит/с", '320kbps': "320 кбит/с",
        '360p': "360p", '480p': "480p", '720p': "720p", '1080p': "1080p",
        processing: "Обрабатываю ваш запрос...",
        inline_processing: "⏳ Обработка...",
        inline_result_title: "{format} - {quality}",
        inline_result_title_tiktok: "{format}",
        inline_invalid_url_prompt: "Неверная ссылка YouTube, Spotify или TikTok",
        inline_description_direct: "Нажмите, чтобы скачать {title} как {format} ({quality})",
        inline_description_spotify: "Нажмите, чтобы скачать трек Spotify: {title} от {artist}",
        inline_description_tiktok: "Нажмите, чтобы скачать видео TikTok как {format}",
        inline_edit_failed: "❌ Не удалось загрузить медиа.",
        inline_cache_upload_failed: "❌ Не удалось подготовить медиа. Попробуйте снова.",
        inline_error_general: "❌ Ошибка обработки запроса.",
        error_fetching_title: "Ошибка получения данных видео.",
        fallback_video_title: "Видео",
        fallback_track_title: "Трек",
        fallback_tiktok_title: "Видео TikTok",
        inline_search_result_title: "{title}",
        inline_search_result_description: "{author} • {views} просмотров • {duration}",
        inline_search_no_results: "Видео по запросу '{query}' не найдены",
        inline_search_prompt: "Введите запрос, ссылку YouTube, Spotify или TikTok...",
        inline_search_select_final: "Видео: {title}\nВыберите формат и качество 👇",
        inline_search_choose_format: "Выбрать Формат",
        inline_search_error: "❌ Ошибка поиска. Попробуйте снова.",
        inline_format_selection: "Video: {title}\nВыберите качество для {format} 👇",
        inline_processing_final: "Video: {title}\nОбработка {format} ({quality})...",
        inline_processing_spotify: "Трек: {title}\nОбработка загрузки Spotify...",
        inline_processing_tiktok: "TikTok: {title}\nОбработка загрузки {format}...",
    }
};

const t = (langOrUserId, key, data = {}) => {
    const lang = userLanguages[langOrUserId] || langOrUserId || defaultLocale;
    const langStrings = translations[lang] || translations[defaultLocale];
    let text = langStrings[key] || `[${key}]`;

    if (key === 'inline_search_result_description' && data.views !== undefined) {
         if (lang === 'ru') {
             const num = data.views;
             let viewsStr = "просмотров";
             if (num % 10 === 1 && num % 100 !== 11) viewsStr = "просмотр";
             else if ([2, 3, 4].includes(num % 10) && ![12, 13, 14].includes(num % 100)) viewsStr = "просмотра";
             data.views = `${num.toLocaleString('ru-RU')} ${viewsStr}`;
         } else {
             data.views = `${data.views.toLocaleString('en-US')}`;
         }
    }

    if (botInfo?.username && text.includes('{botUsername}')) {
        text = text.replace(/\{botUsername\}/g, botInfo.username);
    }

    for (const placeholder in data) {
        const regex = new RegExp(`\\{${placeholder}\\}`, 'g');
        text = text.replace(regex, data[placeholder] !== undefined && data[placeholder] !== null ? data[placeholder] : '');
    }
    return text;
};

const BOT_USERNAME_SUFFIX = () => {
    if (!botInfo?.username) return '';
    return `\n\n@${botInfo.username}`;
};

bot.api.config.use(autoRetry());
bot.api.config.use(apiThrottler());

bot.use(limit({
  timeFrame: 3000, limit: 4,
  onLimitExceeded: async (ctx) => {
    if (ctx?.chat?.type === 'private') {
      const lang = userLanguages[ctx?.from?.id] || defaultLocale;
      const text = lang === 'ru' ? '❌ Слишком много запросов, пожалуйста, подождите.' : '❌ Too many requests, please wait.';
      if (ctx.answerCallbackQuery) {
        await ctx.answerCallbackQuery({ text, show_alert: true }).catch(()=>{});
      } else if (ctx.reply) {
        await ctx.reply(text, { parse_mode: undefined }).catch(()=>{});
      }
    } else if (ctx.inlineQuery) {
        try {
            await ctx.answerInlineQuery([], {
                cache_time: 2,
                switch_pm_text: "Rate limit exceeded, please wait",
                switch_pm_parameter: "rate_limit"
            }).catch(()=>{});
        } catch { }
    }
     else {
        console.warn(`⚠️ Rate limit exceeded in chat ${ctx?.chat?.id} by user ${ctx?.from?.id}`);
    }
  },
  keyGenerator: (ctx) => ctx?.from?.id.toString()
}));

bot.use(async (ctx, next) => {
    const updateType = ctx.updateType;
    const updateId = ctx.update.update_id;

    let user = null;
    let userId = null;
    let fromObject = null;
    let langCode = defaultLocale;
    let userIdSource = "N/A";

    try {
        if (ctx.from) { fromObject = ctx.from; userIdSource = `ctx.from: ${fromObject.id}`; }
        else if (ctx.inlineQuery?.from) { fromObject = ctx.inlineQuery.from; userIdSource = `ctx.inlineQuery.from: ${fromObject.id}`; }
        else if (ctx.chosenInlineResult?.from) { fromObject = ctx.chosenInlineResult.from; userIdSource = `ctx.chosenInlineResult.from: ${fromObject.id}`; }
        else if (ctx.callbackQuery?.from) { fromObject = ctx.callbackQuery.from; userIdSource = `ctx.callbackQuery.from: ${fromObject.id}`; }
        else {
            ctx.lang = defaultLocale;
            ctx.t = (key, data) => t(ctx.lang, key, data);
             console.log(`[Middleware] Update ID ${updateId} (${updateType}): Could not determine user. Using default lang.`);
            await next();
            return;
        }

        userId = fromObject.id;
        langCode = userLanguages[userId] || fromObject.language_code || defaultLocale;
        if (!userLanguages[userId] && fromObject.language_code) {
             userLanguages[userId] = fromObject.language_code.split('-')[0];
             langCode = userLanguages[userId];
             await saveLangCache();
        }
        if (!translations[langCode]) {
            langCode = defaultLocale;
        }


        upsertUser(fromObject, langCode).catch(dbError => {
             console.error(`[Middleware DB Error] Update ID: ${updateId}, User: ${userId} - Error upserting user:`, dbError);
        });

        ctx.lang = langCode;
        ctx.t = (key, data) => t(ctx.lang, key, data);

        await next();

    } catch (error) {
        console.error(`[Middleware CRITICAL ERROR] Update ID: ${updateId}, Type: ${updateType}, User source: ${userIdSource}. Error during middleware processing:`, error);
        if (ctx.reply) {
            await ctx.reply("An internal error occurred in middleware. Please try again later.", { parse_mode: undefined }).catch(()=>{});
        }
    }

    if (ctx.message && fromObject && userId) {
         if (!ctx.message.text?.startsWith('/')) {
            await recordMessage(ctx).catch(recErr => console.error(`[Middleware DB ERROR] Update ID: ${updateId}, User: ${userId} - Error recording message:`, recErr));
         }
    }
});


const audioQualityStrings = ['96kbps', '128kbps', '256kbps', '320kbps'];
const videoQualityStrings = ['360p', '480p', '720p', '1080p'];

const getQualityDisplay = (ctx, qualityString) => {
    return ctx.t(qualityString, qualityString);
};

const replyOpts = () => ({ parse_mode: "HTML", disable_web_page_preview: true });

async function getVideoDetailsSafe(youtubeId, lang) {
    try {
        const videoInfo = await getVideo(youtubeId);
        if (videoInfo) {
            return videoInfo;
        } else {
             console.warn(`[Video Details Fetch] getVideo(${youtubeId}) returned null.`);
             return null;
        }
    } catch (error) {
        console.error(`[Video Details Fetch] Failed for ${youtubeId} using getVideo:`, error.message);
        return null;
    }
}

function getSpotifyTrackId(url) {
    try {
        const trackUrl = new URL(url);
        if (trackUrl.hostname === 'open.spotify.com' && trackUrl.pathname.startsWith('/track/')) {
            const parts = trackUrl.pathname.split('/');
            if (parts.length >= 3 && parts[2]) {
                return parts[2];
            }
        }
    } catch (e) { }
    return null;
}

function isTikTokUrl(url) {
    try {
        const parsedUrl = new URL(url);
        return parsedUrl.hostname.endsWith('tiktok.com');
    } catch (e) {
        return false;
    }
}

async function getTikTokDetailsSafe(tiktokUrl, lang) {
    try {
        const videoInfo = await getTikTokInfo(tiktokUrl, { enableLogging: false });
        if (videoInfo && videoInfo.videoId) {
            return videoInfo;
        } else {
             console.warn(`[TikTok Details Fetch] getTikTokInfo(${tiktokUrl}) returned null or missing videoId.`);
             return null;
        }
    } catch (error) {
        console.error(`[TikTok Details Fetch] Failed for ${tiktokUrl} using getTikTokInfo:`, error.message);
        return null;
    }
}


bot.command("start", async (ctx) => {
    const userId = ctx.from?.id || 'N/A';
    console.log(`[Command /start] User ${userId} used /start. Prompting for language.`);
    const keyboard = new InlineKeyboard()
        .text("English 🇬🇧", "set_lang:en")
        .text("Русский 🇷🇺", "set_lang:ru");
    await ctx.reply(t('en', 'language_select'), {
        reply_markup: keyboard,
        parse_mode: undefined
    });
});

bot.command("stats", async (ctx) => {
    const userId = ctx.from?.id;
    if (userId !== BOT_ADMIN_ID) {
        console.log(`[Command /stats] Unauthorized attempt by user ${userId}`);
        return ctx.reply("⛔️ Access denied.", { parse_mode: undefined });
    }
    console.log(`[Command /stats] Admin ${userId} requested stats.`);
    if (!User || !Message || !Op || !sequelize) {
         console.error("[Command /stats] Database models or Sequelize not initialized.");
         return ctx.reply("⚠️ Error: Database connection or models not ready.", { parse_mode: undefined });
    }
    try {
        await ctx.replyWithChatAction('typing');
        const now = new Date();
        const oneHourAgo = new Date(now.getTime() - 1 * 60 * 60 * 1000);
        const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        const [
            totalUsers, activeLast1h, activeLast24h, activeLast7d, activeLast30d,
            totalMessages, messagesLast1h, messagesLast24h, messagesLast7d, messagesLast30d,
            usersByLangRaw, firstMessageDate, lastMessageDate, cacheSize, langCacheSize
        ] = await Promise.all([
            User.count().catch(() => -1),
            User.count({ where: { lastInteractionAt: { [Op.gte]: oneHourAgo } } }).catch(() => -1),
            User.count({ where: { lastInteractionAt: { [Op.gte]: twentyFourHoursAgo } } }).catch(() => -1),
            User.count({ where: { lastInteractionAt: { [Op.gte]: sevenDaysAgo } } }).catch(() => -1),
            User.count({ where: { lastInteractionAt: { [Op.gte]: thirtyDaysAgo } } }).catch(() => -1),
            Message.count().catch(() => -1),
            Message.count({ where: { messageDate: { [Op.gte]: oneHourAgo } } }).catch(() => -1),
            Message.count({ where: { messageDate: { [Op.gte]: twentyFourHoursAgo } } }).catch(() => -1),
            Message.count({ where: { messageDate: { [Op.gte]: sevenDaysAgo } } }).catch(() => -1),
            Message.count({ where: { messageDate: { [Op.gte]: thirtyDaysAgo } } }).catch(() => -1),
            User.findAll({
                attributes: ['languageCode', [sequelize.fn('COUNT', sequelize.col('userId')), 'count']],
                group: ['languageCode'], raw: true, order: [[sequelize.fn('COUNT', sequelize.col('userId')), 'DESC']]
            }).catch(() => []),
             Message.min('messageDate').catch(() => null),
             Message.max('messageDate').catch(() => null),
             Promise.resolve(Object.keys(fileIdCache).length),
             Promise.resolve(Object.keys(userLanguages).length)
        ]);

        const formatCount = (count) => count === -1 ? 'Error' : count;
        const formatDate = (date) => date ? date.toLocaleString('en-GB', { timeZone: 'UTC' }) + ' UTC' : 'N/A';

        let langStats = "Not available or error fetching";
        if (usersByLangRaw && usersByLangRaw.length > 0) {
            langStats = usersByLangRaw.map(item =>
                `  - ${item.languageCode || 'Unknown'}: ${item.count}`
            ).join('\n');
        } else if (!usersByLangRaw) {
             langStats = "Error fetching language stats";
        } else {
             langStats = "No users found with language preference.";
        }


        const statsMessage = `<b>📊 Bot Statistics</b>\n\n` +
                             `<b>👤 Users (DB):</b>\n` +
                             `  - Total Unique: ${formatCount(totalUsers)}\n` +
                             `  - Active (Last 1h): ${formatCount(activeLast1h)}\n` +
                             `  - Active (Last 24h): ${formatCount(activeLast24h)}\n` +
                             `  - Active (Last 7d): ${formatCount(activeLast7d)}\n` +
                             `  - Active (Last 30d): ${formatCount(activeLast30d)}\n\n` +
                             `<b>💬 Messages (Links Processed):</b>\n` +
                             `  - Total Recorded: ${formatCount(totalMessages)}\n` +
                             `  - Last 1h: ${formatCount(messagesLast1h)}\n` +
                             `  - Last 24h: ${formatCount(messagesLast24h)}\n` +
                             `  - Last 7d: ${formatCount(messagesLast7d)}\n` +
                             `  - Last 30d: ${formatCount(messagesLast30d)}\n\n` +
                             `<b>🌐 Users by Language (DB):</b>\n${langStats}\n\n` +
                             `<b>🗄️ Cache:</b>\n` +
                             `  - File IDs: ${cacheSize}\n` +
                             `  - User Languages: ${langCacheSize}\n\n` +
                             `<b>🕰️ Message Timeline:</b>\n` +
                             `  - First Recorded: ${formatDate(firstMessageDate)}\n`+
                             `  - Last Recorded: ${formatDate(lastMessageDate)}`;

        await ctx.reply(statsMessage, replyOpts());
        console.log(`[Command /stats] Stats sent to admin ${userId}.`);
    } catch (error) {
        console.error("[Command /stats] Error fetching statistics:", error);
        await ctx.reply("⚠️ An error occurred while fetching statistics.", { parse_mode: undefined });
    }
});

bot.callbackQuery(/^set_lang:(en|ru)$/, async (ctx) => {
    const langCode = ctx.match[1];
    const userId = ctx.from.id;
    console.log(`[Callback set_lang] User ${userId} selected language: ${langCode}`);
    userLanguages[userId] = langCode;
    ctx.lang = langCode;
    await saveLangCache();

    upsertUser(ctx.from, langCode).catch(dbError => {
         console.error(`[Callback set_lang] Failed to update language preference in DB for user ${userId}:`, dbError);
    });

    try {
        await ctx.answerCallbackQuery({ text: `Language set to ${langCode === 'en' ? 'English' : 'Русский'}` });
        await ctx.editMessageText(ctx.t('welcome') + BOT_USERNAME_SUFFIX(), {
            ...replyOpts(),
            reply_markup: undefined
        });
    } catch (e) {
        if (!e.description?.includes("modified") && !e.description?.includes("not found")) {
            console.error(`[Callback set_lang] Error processing language change confirmation for ${userId}:`, e);
            await ctx.answerCallbackQuery({ text: "Error setting language", show_alert: true }).catch(()=>{});
        } else {
             await ctx.answerCallbackQuery({ text: `Language set to ${langCode === 'en' ? 'English' : 'Русский'}` }).catch(()=>{});
        }
    }
});

bot.on("message:text", async (ctx) => {
    const userId = ctx.from.id;
    const messageText = ctx.message.text;
    const youtubeId = getYouTubeVideoId(messageText);
    const spotifyTrackId = getSpotifyTrackId(messageText);
    const isTikTok = isTikTokUrl(messageText);

    if (youtubeId) {
        console.log(`[message:text] User ${userId} sent valid YouTube URL (ID: ${youtubeId}). Asking format.`);
        const formatKeyboard = new InlineKeyboard()
            .text(ctx.t('button_mp3'), `fmt_yt:${youtubeId}:mp3`)
            .text(ctx.t('button_mp4'), `fmt_yt:${youtubeId}:mp4`)
            .row()
            .text(ctx.t('button_cancel'), "cancel");
        try {
            await ctx.reply(ctx.t('choose_format'), { parse_mode: undefined, reply_markup: formatKeyboard });
        } catch (e) {
            console.error(`[message:text] User ${userId} - Error sending YT format choice for ${youtubeId}:`, e);
            await ctx.reply(ctx.t('general_error', { error: e.message }) + BOT_USERNAME_SUFFIX(), replyOpts());
        }
    } else if (spotifyTrackId) {
        console.log(`[message:text] User ${userId} sent valid Spotify URL (ID: ${spotifyTrackId}). Starting download.`);
        await handleSpotifyDownloadNormal(ctx, messageText);
    } else if (isTikTok) {
        console.log(`[message:text] User ${userId} sent valid TikTok URL: ${messageText}. Asking format.`);
        const tiktokInfo = await getTikTokDetailsSafe(messageText, ctx.lang);
        if (!tiktokInfo || !tiktokInfo.videoId) {
            console.warn(`[message:text] User ${userId} - Failed to get TikTok info for ${messageText}.`);
            await ctx.reply(ctx.t('tiktok_metadata_failed') + BOT_USERNAME_SUFFIX(), replyOpts());
            return;
        }
        const tiktokVideoId = tiktokInfo.videoId;
        const formatKeyboard = new InlineKeyboard()
            .text(ctx.t('button_mp3'), `fmt_tk:${tiktokVideoId}:mp3`)
            .text(ctx.t('button_mp4'), `fmt_tk:${tiktokVideoId}:mp4`)
            .row()
            .text(ctx.t('button_cancel'), "cancel");
        try {
            const title = tiktokInfo.description ? `"${tiktokInfo.description.substring(0, 50)}..."` : ctx.t('fallback_tiktok_title');
            await ctx.reply(`${ctx.t('choose_format_tiktok')} (${title})`, { parse_mode: undefined, reply_markup: formatKeyboard });
        } catch (e) {
            console.error(`[message:text] User ${userId} - Error sending TikTok format choice for ${tiktokVideoId}:`, e);
            await ctx.reply(ctx.t('general_error', { error: e.message }) + BOT_USERNAME_SUFFIX(), replyOpts());
        }
    }
    else {
        console.log(`[message:text] User ${userId} sent text: "${messageText.substring(0, 50)}..."`);
        if (!messageText.startsWith('/')) {
            await ctx.reply(ctx.t('invalid_url'), replyOpts());
        }
    }
});

async function handleSpotifyDownloadNormal(ctx, trackUrl) {
    const userId = ctx.from.id;
    const lang = ctx.lang;
    const logPrefix = `[Spotify Normal ${userId}]`;
    let statusMessage = null;
    let trackStreamResponse = null;

    try {
        statusMessage = await ctx.reply(ctx.t('processing_spotify'), { parse_mode: undefined });
        await ctx.replyWithChatAction('upload_audio');

        console.log(`${logPrefix} Fetching metadata for ${trackUrl}...`);
        const metadata = await getTrackMetadata(trackUrl, { enableLogging: false });
        if (!metadata || !metadata.name || !metadata.artist) {
            throw new Error(ctx.t('spotify_metadata_failed'));
        }
        console.log(`${logPrefix} Metadata found: "${metadata.name}" by ${metadata.artist}`);

        console.log(`${logPrefix} Requesting download stream...`);
        trackStreamResponse = await downloadSpotifyTrack(trackUrl, null, { enableLogging: false });

        if (!trackStreamResponse || !trackStreamResponse.ok || !trackStreamResponse.body) {
            let errorDetail = `Status: ${trackStreamResponse?.status || 'N/A'}`;
            if (trackStreamResponse && !trackStreamResponse.ok) {
                try { errorDetail += `, Body: ${(await trackStreamResponse.text()).substring(0, 100)}`; } catch { }
            }
            throw new Error(`${ctx.t('spotify_download_failed', { error: 'API stream error' })} (${errorDetail})`);
        }
        console.log(`${logPrefix} Download stream obtained (Content-Type: ${trackStreamResponse.headers.get('content-type')}).`);

        const safeTitle = (metadata.name || ctx.t('fallback_track_title')).replace(/[/\\?%*:|"<>]/g, '-').substring(0, 100);
        const safeArtist = (metadata.artist || 'Unknown Artist').replace(/[/\\?%*:|"<>]/g, '-').substring(0, 50);
        const filename = `${safeArtist} - ${safeTitle}.mp3`;

        const caption = `${metadata.name} - ${metadata.artist}${BOT_USERNAME_SUFFIX()}`;

        console.log(`${logPrefix} Sending audio stream as "${filename}"...`);
        await ctx.replyWithAudio(new InputFile(trackStreamResponse.body, filename), {
            caption: caption,
            parse_mode: "HTML",
            title: metadata.name,
            performer: metadata.artist,
            thumbnail: metadata.cover_url ? new InputFile({ url: metadata.cover_url }) : undefined,
        });
        console.log(`${logPrefix} Successfully sent Spotify track ${metadata.name}`);

        if (statusMessage) {
            await ctx.api.deleteMessage(statusMessage.chat.id, statusMessage.message_id).catch(delErr => {
                if (!delErr.description?.includes("not found")) {
                    console.warn(`${logPrefix} Failed to delete status message ${statusMessage.message_id}:`, delErr.description || delErr);
                }
            });
        }

    } catch (error) {
        console.error(`${logPrefix} Error processing Spotify link ${trackUrl}:`, error);
        const errorMessage = error.message?.includes('extract metadata') || error.message?.includes('Spotify track details')
            ? ctx.t('spotify_metadata_failed')
            : error.message?.includes('download file') || error.message?.includes('API stream error')
            ? ctx.t('spotify_download_failed', { error: error.message })
            : ctx.t('spotify_api_error', { error: error.message });

        if (statusMessage) {
            await ctx.api.editMessageText(statusMessage.chat.id, statusMessage.message_id, errorMessage + BOT_USERNAME_SUFFIX(), replyOpts()).catch(editErr => {
                console.error(`${logPrefix} Failed to edit status message with error:`, editErr);
                ctx.reply(errorMessage + BOT_USERNAME_SUFFIX(), replyOpts()).catch(replyErr => console.error(`${logPrefix} Failed even to send error reply:`, replyErr));
            });
        } else {
            await ctx.reply(errorMessage + BOT_USERNAME_SUFFIX(), replyOpts());
        }
    } finally {
        if (trackStreamResponse?.body && !trackStreamResponse.body.locked && trackStreamResponse.body.cancel) {
             trackStreamResponse.body.cancel().catch(cancelErr => console.warn(`${logPrefix} Error cancelling stream body:`, cancelErr));
        }
    }
}


bot.callbackQuery(/^fmt_yt:([a-zA-Z0-9_-]{11}):(mp3|mp4)$/, async (ctx) => {
    const youtubeId = ctx.match[1];
    const formatString = ctx.match[2];
    const userId = ctx.from.id;
    console.log(`[Callback fmt_yt] User ${userId} chose format ${formatString} for YT ${youtubeId}. Asking quality.`);

    await ctx.answerCallbackQuery();

    let qualityKeyboard = new InlineKeyboard();
    let qualityPrompt;
    let qualityCallbackPrefix = `q_yt:${youtubeId}:${formatString}:`;
    let qualityOptions;

    if (formatString === 'mp3') {
        qualityPrompt = ctx.t('choose_quality_audio');
        qualityOptions = audioQualityStrings;
        qualityOptions.sort((a, b) => parseInt(a) - parseInt(b));
    } else {
        qualityPrompt = ctx.t('choose_quality_video');
        qualityOptions = videoQualityStrings;
        qualityOptions.sort((a, b) => parseInt(a) - parseInt(b));
    }

    qualityOptions.forEach((qualityString, i) => {
        qualityKeyboard.text(getQualityDisplay(ctx, qualityString), `${qualityCallbackPrefix}${qualityString}`);
        if ((i + 1) % 2 === 0) qualityKeyboard.row();
    });
    qualityKeyboard.row().text(ctx.t('button_cancel'), "cancel");

    try {
        await ctx.editMessageText(qualityPrompt, { parse_mode: undefined, reply_markup: qualityKeyboard });
    } catch (e) {
        if (!e.description?.includes("modified")) {
            console.error(`[Callback fmt_yt] User ${userId} - Error editing message for quality choice (${youtubeId}):`, e);
             await ctx.answerCallbackQuery({ text: ctx.t('general_error', { error: 'Failed to show quality options' }), show_alert: true }).catch(()=>{});
        }
    }
});

bot.callbackQuery(/^q_yt:([a-zA-Z0-9_-]{11}):(mp3|mp4):([a-zA-Z0-9]+(?:kbps|p))$/, async (ctx) => {
    const youtubeId = ctx.match[1];
    const formatString = ctx.match[2];
    const chosenQualityString = ctx.match[3];
    const userId = ctx.from.id;
    console.log(`[Callback q_yt] User ${userId} chose quality ${chosenQualityString} (Format: ${formatString}) for YT ${youtubeId}. Starting download process (normal chat).`);

    await ctx.answerCallbackQuery();

    const message = ctx.callbackQuery.message;
    if (!message) {
        console.error(`[Callback q_yt] User ${userId} - CRITICAL: Cannot process quality callback for YT ${youtubeId}: message context missing.`);
        await ctx.answerCallbackQuery({ text: ctx.t('error_unexpected_action'), show_alert: true }).catch(()=>{});
        return;
    }

    const editTarget = { chatId: message.chat.id, messageId: message.message_id };
    const youtubeUrl = `https://www.youtube.com/watch?v=${youtubeId}`;
    await processYouTubeDownloadRequestNormalWithCache(ctx, youtubeId, youtubeUrl, formatString, chosenQualityString, editTarget);
});

bot.callbackQuery(/^fmt_tk:([a-zA-Z0-9_]+):(mp3|mp4)$/, async (ctx) => {
    const tiktokVideoId = ctx.match[1];
    const formatString = ctx.match[2];
    const userId = ctx.from.id;
    console.log(`[Callback fmt_tk] User ${userId} chose format ${formatString} for TikTok ${tiktokVideoId}. Starting download process (normal chat).`);

    await ctx.answerCallbackQuery();

    const message = ctx.callbackQuery.message;
    if (!message) {
        console.error(`[Callback fmt_tk] User ${userId} - CRITICAL: Cannot process format callback for TikTok ${tiktokVideoId}: message context missing.`);
        await ctx.answerCallbackQuery({ text: ctx.t('error_unexpected_action'), show_alert: true }).catch(()=>{});
        return;
    }

    const originalMessageText = message.reply_to_message?.text || message.text;
    let tiktokUrl = null;
    if (originalMessageText && isTikTokUrl(originalMessageText)) {
        tiktokUrl = originalMessageText;
    } else {
        tiktokUrl = `https://www.tiktok.com/video/${tiktokVideoId}`;
        console.warn(`[Callback fmt_tk] User ${userId} - Could not reliably get original TikTok URL for ${tiktokVideoId}. Using fallback: ${tiktokUrl}`);
    }

    if (!tiktokUrl) {
         console.error(`[Callback fmt_tk] User ${userId} - CRITICAL: Could not determine TikTok URL for ${tiktokVideoId}.`);
         await ctx.answerCallbackQuery({ text: ctx.t('error_unexpected_action'), show_alert: true }).catch(()=>{});
         try { await ctx.editMessageText(ctx.t('error_unexpected_action'), { parse_mode: undefined, reply_markup: undefined }); } catch {}
         return;
    }


    const editTarget = { chatId: message.chat.id, messageId: message.message_id };
    await processTikTokDownloadRequestNormalWithCache(ctx, tiktokVideoId, tiktokUrl, formatString, editTarget);
});


bot.callbackQuery("cancel", async (ctx) => {
    const userId = ctx.from.id;
    console.log(`[Callback cancel] User ${userId} cancelled the action.`);
    await ctx.answerCallbackQuery();
    try {
        await ctx.editMessageText(ctx.t("action_cancelled"), { parse_mode: undefined, reply_markup: undefined });
    } catch (e) {
        if (!e.description?.includes("modified") && !e.description?.includes("not found")) {
            console.error(`[Callback cancel] User ${userId} - Error editing message on cancel:`, e);
        }
    }
});

const processingKeyboard = new InlineKeyboard().text("⏳", "inline_ignore");

bot.on("inline_query", async (ctx) => {
    const query = ctx.inlineQuery.query.trim();
    const userId = ctx.inlineQuery.from.id;
    const offset = parseInt(ctx.inlineQuery.offset, 10) || 0;
    const lang = ctx.lang || defaultLocale;

    if (!query) {
        try {
            await ctx.answerInlineQuery([], {
                cache_time: 10,
                switch_pm_text: ctx.t('inline_search_prompt'),
                switch_pm_parameter: "inline_help"
            });
        } catch (e) { console.error(`[Inline Query ${userId}] Error answering empty query prompt:`, e); }
        return;
    }

    const youtubeId = getYouTubeVideoId(query);
    const spotifyTrackId = getSpotifyTrackId(query);
    const isTikTok = isTikTokUrl(query);

    if (youtubeId) {
        console.log(`[Inline Query ${userId}] Received valid YouTube URL for ID: ${youtubeId}. Generating YT results...`);
        try {
            const videoDetails = await getVideoDetailsSafe(youtubeId, lang);
            const videoTitle = videoDetails?.title || ctx.t('fallback_video_title');

            if (!videoDetails) {
                 console.warn(`[Inline Query ${userId}] Could not fetch YT details for ${youtubeId}. Sending error.`);
                 await ctx.answerInlineQuery([{
                     type: "article", id: `error_yt:${youtubeId}`, title: ctx.t('error_fetching_title'),
                     input_message_content: { message_text: ctx.t('error_fetching_title'), parse_mode: undefined }
                 }], { cache_time: 5 });
                 return;
            }

            const results = [];
            const initialMessageText = ctx.t('inline_processing');

            audioQualityStrings.sort((a, b) => parseInt(a) - parseInt(b)).forEach((qualityString) => {
                const qualityName = getQualityDisplay(ctx, qualityString);
                const resultId = `dl_yt:${youtubeId}:mp3:${qualityString}`;
                results.push({
                    type: "article", id: resultId,
                    title: ctx.t('inline_result_title', { format: "MP3", quality: qualityName }),
                    description: ctx.t('inline_description_direct', { title: videoTitle, format: "MP3", quality: qualityName }),
                    reply_markup: processingKeyboard,
                    input_message_content: { message_text: initialMessageText, parse_mode: "HTML", disable_web_page_preview: true },
                    thumbnail_url: videoDetails.thumbnail || undefined,
                });
            });

            videoQualityStrings.sort((a, b) => parseInt(a) - parseInt(b)).forEach((qualityString) => {
                const qualityName = getQualityDisplay(ctx, qualityString);
                const resultId = `dl_yt:${youtubeId}:mp4:${qualityString}`;
                results.push({
                    type: "article", id: resultId,
                    title: ctx.t('inline_result_title', { format: "MP4", quality: qualityName }),
                    description: ctx.t('inline_description_direct', { title: videoTitle, format: "MP4", quality: qualityName }),
                    reply_markup: processingKeyboard,
                    input_message_content: { message_text: initialMessageText, parse_mode: "HTML", disable_web_page_preview: true },
                    thumbnail_url: videoDetails.thumbnail || undefined,
                });
            });

            await ctx.answerInlineQuery(results, { cache_time: 60 });
            console.log(`[Inline Query ${userId}] Sent ${results.length} YT download results for ${youtubeId}`);
        } catch (error) {
            console.error(`[Inline Query ${userId}] Error processing YT link ${youtubeId}:`, error);
            try { await ctx.answerInlineQuery([], { cache_time: 5 }); } catch { }
        }
        return;
    }

    if (spotifyTrackId) {
        console.log(`[Inline Query ${userId}] Received valid Spotify URL for ID: ${spotifyTrackId}. Generating Spotify result...`);
        try {
            const metadata = await getTrackMetadata(query, { enableLogging: false });
            if (!metadata || !metadata.name || !metadata.artist) {
                 console.warn(`[Inline Query ${userId}] Could not fetch Spotify details for ${spotifyTrackId}. Sending error.`);
                 await ctx.answerInlineQuery([{
                     type: "article", id: `error_spotify:${spotifyTrackId}`, title: ctx.t('spotify_metadata_failed'),
                     input_message_content: { message_text: ctx.t('spotify_metadata_failed'), parse_mode: undefined }
                 }], { cache_time: 5 });
                 return;
            }

            const resultId = `dl_spotify:${spotifyTrackId}`;
            const trackTitle = metadata.name || ctx.t('fallback_track_title');
            const artistName = metadata.artist || 'Unknown Artist';

            const result = {
                type: "article",
                id: resultId,
                title: trackTitle,
                description: ctx.t('inline_description_spotify', { title: trackTitle, artist: artistName }),
                thumbnail_url: metadata.cover_url || undefined,
                reply_markup: processingKeyboard,
                input_message_content: {
                    message_text: ctx.t('inline_processing'),
                    parse_mode: "HTML",
                    disable_web_page_preview: true
                },
            };

            await ctx.answerInlineQuery([result], { cache_time: 60 });
            console.log(`[Inline Query ${userId}] Sent Spotify download result for ${spotifyTrackId}`);

        } catch (error) {
            console.error(`[Inline Query ${userId}] Error processing Spotify link ${spotifyTrackId}:`, error);
            await ctx.answerInlineQuery([{
                 type: "article", id: `error_spotify:${spotifyTrackId}`, title: ctx.t('spotify_api_error', {error: ''}),
                 input_message_content: { message_text: ctx.t('spotify_api_error', {error: error.message}), parse_mode: undefined }
            }], { cache_time: 5 }).catch(()=>{});
        }
        return;
    }

    if (isTikTok) {
        console.log(`[Inline Query ${userId}] Received valid TikTok URL: ${query}. Generating TikTok results...`);
        try {
            const tiktokInfo = await getTikTokDetailsSafe(query, lang);
            if (!tiktokInfo || !tiktokInfo.videoId) {
                 console.warn(`[Inline Query ${userId}] Could not fetch TikTok details for ${query}. Sending error.`);
                 await ctx.answerInlineQuery([{
                     type: "article", id: `error_tk:${Date.now()}`, title: ctx.t('tiktok_metadata_failed'),
                     input_message_content: { message_text: ctx.t('tiktok_metadata_failed'), parse_mode: undefined }
                 }], { cache_time: 5 });
                 return;
            }

            const tiktokVideoId = tiktokInfo.videoId;
            const videoTitle = tiktokInfo.description?.substring(0, 70) || ctx.t('fallback_tiktok_title');
            const results = [];
            const initialMessageText = ctx.t('inline_processing');

            results.push({
                type: "article", id: `dl_tk:${tiktokVideoId}:mp3`,
                title: ctx.t('inline_result_title_tiktok', { format: "MP3" }),
                description: ctx.t('inline_description_tiktok', { title: videoTitle, format: "MP3" }),
                reply_markup: processingKeyboard,
                input_message_content: { message_text: initialMessageText, parse_mode: "HTML", disable_web_page_preview: true },
                thumbnail_url: tiktokInfo.thumbnailUrl || undefined,
            });

            results.push({
                type: "article", id: `dl_tk:${tiktokVideoId}:mp4`,
                title: ctx.t('inline_result_title_tiktok', { format: "MP4" }),
                description: ctx.t('inline_description_tiktok', { title: videoTitle, format: "MP4" }),
                reply_markup: processingKeyboard,
                input_message_content: { message_text: initialMessageText, parse_mode: "HTML", disable_web_page_preview: true },
                thumbnail_url: tiktokInfo.thumbnailUrl || undefined,
            });

            await ctx.answerInlineQuery(results, { cache_time: 60 });
            console.log(`[Inline Query ${userId}] Sent ${results.length} TikTok download results for ${tiktokVideoId}`);
        } catch (error) {
            console.error(`[Inline Query ${userId}] Error processing TikTok link ${query}:`, error);
             await ctx.answerInlineQuery([{
                 type: "article", id: `error_tk:${Date.now()}`, title: ctx.t('tiktok_api_error', {error: ''}),
                 input_message_content: { message_text: ctx.t('tiktok_api_error', {error: error.message}), parse_mode: undefined }
            }], { cache_time: 5 }).catch(()=>{});
        }
        return;
    }


    console.log(`[Inline Query ${userId}] Received YT search query: "${query}". Offset: ${offset}. Searching videos...`);
    try {
        const searchResults = await searchVideos(query, { hl: lang, gl: config.DEFAULT_GL || 'US', pageEnd: 1 });

        if (!searchResults || searchResults.length === 0) {
            console.log(`[Inline Query ${userId}] No YT search results found for "${query}".`);
            await ctx.answerInlineQuery([{
                type: "article", id: "no_results", title: ctx.t('inline_search_no_results', { query: query }),
                input_message_content: { message_text: ctx.t('inline_search_no_results', { query: query }), parse_mode: undefined }
            }], { cache_time: 10 });
            return;
        }

        console.log(`[Inline Query ${userId}] Found ${searchResults.length} YT videos for "${query}". Mapping...`);

        const results = searchResults
            .slice(0, INLINE_SEARCH_LIMIT)
            .map((video) => {
                const resultId = `srch_res_yt:${video.videoId}`;
                const viewsText = video.views?.toLocaleString(lang === 'ru' ? 'ru-RU' : 'en-US') ?? '?';
                const videoTitle = video.title || ctx.t('fallback_video_title');

                const combinedKeyboard = new InlineKeyboard();
                const buttonRows = [];
                audioQualityStrings.forEach(q => buttonRows.push({ text: `MP3 ${getQualityDisplay(ctx, q)}`, callback_data: `inline_dl_yt:${video.videoId}:mp3:${q}` }));
                videoQualityStrings.forEach(q => buttonRows.push({ text: `MP4 ${getQualityDisplay(ctx, q)}`, callback_data: `inline_dl_yt:${video.videoId}:mp4:${q}` }));
                for (let i = 0; i < buttonRows.length; i += 2) {
                    combinedKeyboard.row(...buttonRows.slice(i, i + 2).map(btn => InlineKeyboard.text(btn.text, btn.callback_data)));
                }

                return {
                    type: "article", id: resultId, title: videoTitle,
                    description: ctx.t('inline_search_result_description', { author: video.author?.name || 'Unknown', views: viewsText, duration: video.timestamp || '?:??' }),
                    thumbnail_url: video.thumbnail || undefined,
                    input_message_content: { message_text: ctx.t('inline_search_select_final', { title: videoTitle }), parse_mode: "HTML", disable_web_page_preview: true },
                    reply_markup: combinedKeyboard,
                };
            });

        await ctx.answerInlineQuery(results, { cache_time: 30 });
        console.log(`[Inline Query ${userId}] Sent ${results.length} YT search results for "${query}".`);

    } catch (error) {
        console.error(`[Inline Query ${userId}] Error during YouTube search for "${query}":`, error);
        try {
            await ctx.answerInlineQuery([{
                type: "article", id: "search_error", title: ctx.t('inline_search_error'),
                input_message_content: { message_text: ctx.t('inline_search_error'), parse_mode: undefined }
            }], { cache_time: 5 });
        } catch (e) { console.error(`[Inline Query ${userId}] Failed to answer with search error:`, e); }
    }
});


bot.on("chosen_inline_result", async (ctx) => {
    const resultId = ctx.chosenInlineResult.result_id;
    const inlineMessageId = ctx.chosenInlineResult.inline_message_id;
    const userId = ctx.chosenInlineResult.from.id;
    const query = ctx.chosenInlineResult.query;
    const lang = ctx.lang || defaultLocale;

    console.log(`[Chosen Inline ${userId}] Result ID: ${resultId}, InlineMsgID: ${inlineMessageId}, Query: "${query}"`);

    if (!inlineMessageId) {
        console.error(`[Chosen Inline ${userId}] CRITICAL: No inline_message_id received for result_id: ${resultId}. Cannot proceed.`);
        return;
    }

    const directDownloadMatchYT = resultId.match(/^dl_yt:([a-zA-Z0-9_-]{11}):(mp3|mp4):([a-zA-Z0-9]+(?:kbps|p))$/);
    if (directDownloadMatchYT) {
        const [, youtubeId, formatString, chosenQualityString] = directDownloadMatchYT;
        const cacheKey = `yt:${youtubeId}:${formatString}:${chosenQualityString}`;
        console.log(`[Chosen Inline ${userId}] YT Direct DL chosen - ID: ${youtubeId}, Format: ${formatString}, Quality: ${chosenQualityString}. Cache key: ${cacheKey}`);

        const videoDetails = await getVideoDetailsSafe(youtubeId, lang);
        const videoTitle = videoDetails?.title || ctx.t('fallback_video_title');
        const qualityDisplayName = getQualityDisplay(ctx, chosenQualityString);

        try {
            await ctx.api.editMessageTextInline(
                inlineMessageId,
                ctx.t('inline_processing_final', { title: videoTitle, format: formatString.toUpperCase(), quality: qualityDisplayName }),
                { reply_markup: processingKeyboard, parse_mode: "HTML" }
            ).catch(e => { if (!e.description?.includes("modified")) console.warn(`[Chosen Inline YT dl:] Edit failed:`, e.description || e); });
        } catch (editErr) { console.error(`[Chosen Inline YT dl:] Error editing message ${inlineMessageId}:`, editErr); }

        const cachedFileId = fileIdCache[cacheKey];
        if (cachedFileId && videoDetails) {
            console.log(`[Chosen Inline ${userId}] Cache HIT for YT ${cacheKey}. Editing message.`);
            await editInlineMessageWithFileId(ctx, inlineMessageId, cachedFileId, 'yt_' + formatString, videoDetails);
        } else {
            console.log(`[Chosen Inline ${userId}] Cache MISS for YT ${cacheKey}. Starting download & cache.`);
            if (videoDetails) {
                await processYouTubeDownloadAndCache(ctx, youtubeId, formatString, chosenQualityString, inlineMessageId, cacheKey, videoDetails);
            } else {
                console.error(`[Chosen Inline ${userId}] Cannot process YT ${cacheKey}: Failed to get video details.`);
                await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('error_fetching_title'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
            }
        }
        return;
    }

    const spotifyDownloadMatch = resultId.match(/^dl_spotify:([a-zA-Z0-9]+)$/);
    if (spotifyDownloadMatch) {
        const [, spotifyTrackId] = spotifyDownloadMatch;
        const cacheKey = `spotify:${spotifyTrackId}`;
        console.log(`[Chosen Inline ${userId}] Spotify DL chosen - ID: ${spotifyTrackId}. Cache key: ${cacheKey}`);

        let metadata;
        try {
             metadata = await getTrackMetadata(`https://open.spotify.com/track/${spotifyTrackId}`, { enableLogging: false });
        } catch (metaErr) {
             console.error(`[Chosen Inline Spotify] Failed to get metadata for ${spotifyTrackId}:`, metaErr);
             await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('spotify_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
             return;
        }

        const trackTitle = metadata?.name || ctx.t('fallback_track_title');

        try {
            await ctx.api.editMessageTextInline(
                inlineMessageId,
                ctx.t('inline_processing_spotify', { title: trackTitle }),
                { reply_markup: processingKeyboard, parse_mode: "HTML" }
            ).catch(e => { if (!e.description?.includes("modified")) console.warn(`[Chosen Inline Spotify dl:] Edit failed:`, e.description || e); });
        } catch (editErr) { console.error(`[Chosen Inline Spotify dl:] Error editing message ${inlineMessageId}:`, editErr); }

        const cachedFileId = fileIdCache[cacheKey];
        if (cachedFileId && metadata) {
            console.log(`[Chosen Inline ${userId}] Cache HIT for Spotify ${cacheKey}. Editing message.`);
            await editInlineMessageWithFileId(ctx, inlineMessageId, cachedFileId, 'spotify', metadata);
        } else {
            console.log(`[Chosen Inline ${userId}] Cache MISS for Spotify ${cacheKey}. Starting download & cache.`);
            if (metadata) {
                await processSpotifyDownloadAndCache(ctx, spotifyTrackId, inlineMessageId, cacheKey, metadata);
            } else {
                console.error(`[Chosen Inline ${userId}] Cannot process Spotify ${cacheKey}: Metadata missing.`);
                await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('spotify_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
            }
        }
        return;
    }

    const directDownloadMatchTK = resultId.match(/^dl_tk:([a-zA-Z0-9_]+):(mp3|mp4)$/);
    if (directDownloadMatchTK) {
        const [, tiktokVideoId, formatString] = directDownloadMatchTK;
        const cacheKey = `tk:${tiktokVideoId}:${formatString}`;
        console.log(`[Chosen Inline ${userId}] TikTok Direct DL chosen - ID: ${tiktokVideoId}, Format: ${formatString}. Cache key: ${cacheKey}`);

        let tiktokInfo;
        let tiktokUrl = query;
        try {
            if (!isTikTokUrl(tiktokUrl)) {
                tiktokUrl = `https://www.tiktok.com/video/${tiktokVideoId}`;
                console.warn(`[Chosen Inline TikTok dl:] Query "${query}" is not a TikTok URL. Using fallback: ${tiktokUrl}`);
            }
            tiktokInfo = await getTikTokDetailsSafe(tiktokUrl, lang);
        } catch (infoErr) {
            console.error(`[Chosen Inline TikTok dl:] Failed to get TikTok info for ${tiktokVideoId} using URL ${tiktokUrl}:`, infoErr);
            await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('tiktok_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
            return;
        }

        if (!tiktokInfo) {
            console.error(`[Chosen Inline TikTok dl:] getTikTokDetailsSafe returned null for ${tiktokVideoId}.`);
            await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('tiktok_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
            return;
        }

        const videoTitle = tiktokInfo.description?.substring(0, 70) || ctx.t('fallback_tiktok_title');

        try {
            await ctx.api.editMessageTextInline(
                inlineMessageId,
                ctx.t('inline_processing_tiktok', { title: videoTitle, format: formatString.toUpperCase() }),
                { reply_markup: processingKeyboard, parse_mode: "HTML" }
            ).catch(e => { if (!e.description?.includes("modified")) console.warn(`[Chosen Inline TikTok dl:] Edit failed:`, e.description || e); });
        } catch (editErr) { console.error(`[Chosen Inline TikTok dl:] Error editing message ${inlineMessageId}:`, editErr); }

        const cachedFileId = fileIdCache[cacheKey];
        if (cachedFileId) {
            console.log(`[Chosen Inline ${userId}] Cache HIT for TikTok ${cacheKey}. Editing message.`);
            await editInlineMessageWithFileId(ctx, inlineMessageId, cachedFileId, 'tk_' + formatString, tiktokInfo);
        } else {
            console.log(`[Chosen Inline ${userId}] Cache MISS for TikTok ${cacheKey}. Starting download & cache.`);
            await processTikTokDownloadAndCache(ctx, tiktokVideoId, tiktokUrl, formatString, inlineMessageId, cacheKey, tiktokInfo);
        }
        return;
    }


    const searchResultMatchYT = resultId.match(/^srch_res_yt:([a-zA-Z0-9_-]{11})$/);
    if (searchResultMatchYT) {
        const [, youtubeId] = searchResultMatchYT;
        console.log(`[Chosen Inline ${userId}] YT Search result chosen for ${youtubeId}. Message ${inlineMessageId} now shows quality options.`);
        return;
    }

    console.error(`[Chosen Inline ${userId}] Invalid or unexpected result_id format: ${resultId}. Editing message to error.`);
    try {
        await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('error_unexpected_action'), { reply_markup: undefined, parse_mode: undefined });
    } catch (e) {
        if (!e.description?.includes("not found") && !e.description?.includes("can't be edited") && !e.description?.includes("is invalid")) {
            console.error(`[Chosen Inline ${userId}] Failed to edit inline message [${inlineMessageId}] with error:`, e.description || e);
        }
    }
});

bot.callbackQuery(/^inline_dl_yt:([a-zA-Z0-9_-]{11}):(mp3|mp4):([a-zA-Z0-9]+(?:kbps|p))$/, async (ctx) => {
    const youtubeId = ctx.match[1];
    const formatString = ctx.match[2];
    const chosenQualityString = ctx.match[3];
    const userId = ctx.from.id;
    const inlineMessageId = ctx.callbackQuery.inline_message_id;
    const lang = ctx.lang || defaultLocale;

    if (!inlineMessageId) {
        console.error(`[Callback inline_dl_yt] User ${userId} - CRITICAL: No inline_message_id for YT ${youtubeId}:${formatString}:${chosenQualityString}.`);
        await ctx.answerCallbackQuery({ text: ctx.t('error_unexpected_action'), show_alert: true });
        return;
    }

    console.log(`[Callback inline_dl_yt] User ${userId} chose quality ${chosenQualityString} (Format: ${formatString}) for YT ${youtubeId} via inline msg ${inlineMessageId}.`);

    await ctx.answerCallbackQuery({ text: ctx.t('requesting_download') });

    try {
         const videoDetails = await getVideoDetailsSafe(youtubeId, lang);
         const videoTitle = videoDetails?.title || ctx.t('fallback_video_title');
         const qualityDisplayName = getQualityDisplay(ctx, chosenQualityString);

         await ctx.api.editMessageTextInline(
             inlineMessageId,
             ctx.t('inline_processing_final', { title: videoTitle, format: formatString.toUpperCase(), quality: qualityDisplayName }),
             { reply_markup: processingKeyboard, parse_mode: "HTML" }
         ).catch(e => { if (!e.description?.includes("modified")) console.warn(`[Callback inline_dl_yt] Edit failed:`, e.description || e); });

        const cacheKey = `yt:${youtubeId}:${formatString}:${chosenQualityString}`;
        const cachedFileId = fileIdCache[cacheKey];

        if (cachedFileId && videoDetails) {
            console.log(`[Callback inline_dl_yt] Cache HIT for ${cacheKey}. Editing message ${inlineMessageId}.`);
            await editInlineMessageWithFileId(ctx, inlineMessageId, cachedFileId, 'yt_' + formatString, videoDetails);
        } else {
            console.log(`[Callback inline_dl_yt] Cache MISS for ${cacheKey}. Starting download & cache for ${inlineMessageId}.`);
            if (videoDetails) {
                await processYouTubeDownloadAndCache(ctx, youtubeId, formatString, chosenQualityString, inlineMessageId, cacheKey, videoDetails);
            } else {
                 console.error(`[Callback inline_dl_yt] Cannot process ${cacheKey}: Failed to get YT details.`);
                 await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('error_fetching_title'), { reply_markup: undefined, parse_mode: undefined }).catch(()=>{});
            }
        }
    } catch (error) {
         console.error(`[Callback inline_dl_yt] User ${userId} - Error handling YT quality selection for ${inlineMessageId}:`, error);
         try {
             await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('inline_error_general'), { reply_markup: undefined, parse_mode: undefined });
         } catch (editError) {
             if (!editError.description?.includes("not found")) console.error(`[Callback inline_dl_yt] Failed to set error state for ${inlineMessageId}:`, editError.description || editError);
         }
    }
});

bot.callbackQuery("inline_ignore", async (ctx) => {
    await ctx.answerCallbackQuery();
});

async function editInlineMessageWithFileId(ctx, inlineMessageId, file_id, formatString, detailsObject) {
    const userId = ctx.from?.id || ctx.chosenInlineResult?.from?.id || ctx.callbackQuery?.from?.id || 'N/A';
    const lang = ctx.lang || userLanguages[userId] || defaultLocale;
    const logPrefix = `[Edit Inline ${userId}]`;
    console.log(`${logPrefix} Attempting to edit inline message ${inlineMessageId} using file_id ${file_id} (Format: ${formatString})`);

    if (!detailsObject) {
        console.error(`${logPrefix} Cannot edit inline message ${inlineMessageId}: detailsObject is missing.`);
        let errorKey = 'general_error';
        if (formatString.startsWith('yt_')) errorKey = 'error_fetching_title';
        else if (formatString === 'spotify') errorKey = 'spotify_metadata_failed';
        else if (formatString.startsWith('tk_')) errorKey = 'tiktok_metadata_failed';
        try {
            await ctx.api.editMessageTextInline(inlineMessageId, ctx.t(errorKey), { reply_markup: undefined, parse_mode: undefined });
        } catch (e) { }
        return;
    }

    try {
        let caption = '';
        let inputMedia;
        const baseMediaOptions = { parse_mode: "HTML" };

        if (formatString === 'spotify') {
            const metadata = detailsObject;
            const trackTitle = metadata.name || ctx.t('fallback_track_title');
            const artistName = metadata.artist || 'Unknown Artist';
            caption = `${trackTitle} - ${artistName}${BOT_USERNAME_SUFFIX()}`;
            inputMedia = InputMediaBuilder.audio(file_id, {
                ...baseMediaOptions, caption: caption, title: trackTitle, performer: artistName,
            });
            console.log(`${logPrefix} Preparing InputMediaAudio for Spotify track "${trackTitle}"`);

        } else if (formatString === 'yt_mp3') {
            const videoDetails = detailsObject;
            const videoTitle = videoDetails.title || ctx.t('fallback_video_title');
            const videoUrl = `https://www.youtube.com/watch?v=${videoDetails.videoId}`;
            caption = `${videoTitle}\n${videoUrl}${BOT_USERNAME_SUFFIX()}`;
            inputMedia = InputMediaBuilder.audio(file_id, {
                ...baseMediaOptions, caption: caption, duration: videoDetails.seconds, performer: videoDetails.author?.name, title: videoTitle
            });
             console.log(`${logPrefix} Preparing InputMediaAudio for YT "${videoTitle}", Duration: ${videoDetails.seconds}`);

        } else if (formatString === 'yt_mp4') {
            const videoDetails = detailsObject;
            const videoTitle = videoDetails.title || ctx.t('fallback_video_title');
            const videoUrl = `https://www.youtube.com/watch?v=${videoDetails.videoId}`;
            caption = `${videoTitle}\n${videoUrl}${BOT_USERNAME_SUFFIX()}`;
            inputMedia = InputMediaBuilder.video(file_id, {
                ...baseMediaOptions, caption: caption, duration: videoDetails.seconds, supports_streaming: true
            });
            console.log(`${logPrefix} Preparing InputMediaVideo for YT "${videoTitle}", Duration: ${videoDetails.seconds}`);

        } else if (formatString === 'tk_mp3') {
            const tiktokInfo = detailsObject;
            const videoTitle = tiktokInfo.description?.substring(0, 150) || ctx.t('fallback_tiktok_title');
            const videoUrl = `https://www.tiktok.com/video/${tiktokInfo.videoId}`;
            caption = `${videoTitle}\n${videoUrl}${BOT_USERNAME_SUFFIX()}`;
            inputMedia = InputMediaBuilder.audio(file_id, {
                ...baseMediaOptions, caption: caption, title: videoTitle,
            });
            console.log(`${logPrefix} Preparing InputMediaAudio for TikTok "${videoTitle}"`);

        } else if (formatString === 'tk_mp4') {
            const tiktokInfo = detailsObject;
            const videoTitle = tiktokInfo.description?.substring(0, 150) || ctx.t('fallback_tiktok_title');
            const videoUrl = `https://www.tiktok.com/video/${tiktokInfo.videoId}`;
            caption = `${videoTitle}\n${videoUrl}${BOT_USERNAME_SUFFIX()}`;
            inputMedia = InputMediaBuilder.video(file_id, {
                ...baseMediaOptions, caption: caption, supports_streaming: true,
            });
            console.log(`${logPrefix} Preparing InputMediaVideo for TikTok "${videoTitle}"`);
        }
         else {
             console.error(`${logPrefix} Unknown formatString "${formatString}" in editInlineMessageWithFileId.`);
             throw new Error("Internal error: Unknown format for editing.");
         }

        console.log(`${logPrefix} Calling editMessageMediaInline for ${inlineMessageId}.`);
        await ctx.api.editMessageMediaInline(inlineMessageId, inputMedia, {
            reply_markup: new InlineKeyboard()
        });
        console.log(`${logPrefix} Successfully edited inline message ${inlineMessageId} with file ${file_id}.`);

    } catch (error) {
        console.error(`${logPrefix} FAILED to edit inline message ${inlineMessageId} with file_id ${file_id} (Format: ${formatString}):`, error.description || error);
        try {
            await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('inline_edit_failed'), {
                reply_markup: undefined, parse_mode: undefined
            });
        } catch (editError) {
             if (!editError.description?.includes("not found") && !editError.description?.includes("can't be edited") && !editError.description?.includes("is invalid")) {
                console.error(`${logPrefix} Failed even to edit inline message ${inlineMessageId} to error text:`, editError.description || editError);
             }
        }
    }
}

async function processYouTubeDownloadAndCache(ctx, youtubeId, formatString, chosenQualityString, inlineMessageId, cacheKey, videoDetails) {
    const userId = ctx.from?.id || ctx.chosenInlineResult?.from?.id || ctx.callbackQuery?.from?.id || 'N/A';
    const logPrefix = `[YT Download&Cache ${userId}]`;
    const canonicalUrl = `https://www.youtube.com/watch?v=${youtubeId}`;
    let fileStreamResponse = null;
    console.log(`${logPrefix} Starting YT process for ${youtubeId}, format: ${formatString}, quality: ${chosenQualityString}. Target: ${TARGET_CHANNEL_ID}, InlineMsgID: ${inlineMessageId}, CacheKey: ${cacheKey}`);

    if (!videoDetails) {
        console.error(`${logPrefix} Cannot process YT download/cache for ${inlineMessageId}: videoDetails missing.`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('error_fetching_title'), { reply_markup: undefined, parse_mode: undefined }); } catch (e) { }
        return;
    }

    const setInlineError = async (errorKey = 'inline_error_general', templateData = {}) => {
        console.error(`${logPrefix} Setting inline message ${inlineMessageId} to YT error state (${errorKey})`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t(errorKey, templateData), { reply_markup: undefined, parse_mode: "HTML" }); }
        catch (e) { if (!e.description?.includes("not found") && !e.description?.includes("can't be edited")) console.error(`${logPrefix} Failed to edit inline message ${inlineMessageId} to YT error state '${errorKey}':`, e.description || e); }
    };

    try {
        console.log(`${logPrefix} Calling downloadYouTubeVideo(${canonicalUrl}, ${formatString}, ${chosenQualityString})`);
        fileStreamResponse = await downloadYouTubeVideo(canonicalUrl, formatString, chosenQualityString, null, { enableLogging: true });
        console.log(`${logPrefix} Received response from downloadYouTubeVideo. Status: ${fileStreamResponse?.status}`);

        if (!fileStreamResponse || !fileStreamResponse.body || !fileStreamResponse.ok) {
             let apiErrorMsg = `Status: ${fileStreamResponse?.status || 'N/A'}`;
             if (fileStreamResponse && !fileStreamResponse.ok) try { apiErrorMsg += `, Body: ${(await fileStreamResponse.text()).substring(0, 100)}`; } catch { }
            if (apiErrorMsg.includes("Video is too long")) throw new Error("Video is too long");
            throw new Error(`YT Download service failed. ${apiErrorMsg}`);
        }

        console.log(`${logPrefix} YT File stream obtained. Sending to channel ${TARGET_CHANNEL_ID}...`);
        let filename = `${(videoDetails.title || youtubeId).substring(0, 100)}_${formatString}_${chosenQualityString}.${formatString}`;
        const contentDisposition = fileStreamResponse.headers.get('content-disposition');
         if (contentDisposition) {
             const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i); if (utf8Match?.[1]) try { filename = decodeURIComponent(utf8Match[1]); } catch {}
             else { const asciiMatch = contentDisposition.match(/filename="?([^";]+)"?/i); if (asciiMatch?.[1]) filename = asciiMatch[1]; }
         }
         filename = filename.replace(/[/\\?%*:|"<>]/g, '-').substring(0, 200);
        console.log(`${logPrefix} Using filename for YT channel upload: ${filename}`);

        const inputFile = new InputFile(fileStreamResponse.body, filename);
        const channelCaption = `Cache YT: ${youtubeId} | ${formatString} | ${chosenQualityString}`;
        const sendOptions = { caption: channelCaption, disable_notification: true, parse_mode: undefined };

        let sentMessage;
        console.log(`${logPrefix} Sending YT ${formatString} to channel...`);
        if (formatString === 'mp3') {
            sentMessage = await ctx.api.sendAudio(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, duration: videoDetails.seconds, performer: videoDetails.author?.name, title: videoDetails.title });
        } else {
            sentMessage = await ctx.api.sendVideo(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, duration: videoDetails.seconds, supports_streaming: true, thumbnail: videoDetails.thumbnail ? new InputFile({ url: videoDetails.thumbnail }) : undefined });
        }
        console.log(`${logPrefix} Successfully sent YT file to channel ${TARGET_CHANNEL_ID}. Message ID: ${sentMessage.message_id}`);

        const file_id = formatString === 'mp3' ? sentMessage.audio?.file_id : sentMessage.video?.file_id;
        if (!file_id) throw new Error("Failed to get YT file_id after channel upload.");
        console.log(`${logPrefix} Extracted YT file_id: ${file_id}`);

        fileIdCache[cacheKey] = file_id; await saveCache();
        console.log(`${logPrefix} Saved YT file_id ${file_id} to cache with key ${cacheKey}.`);

        await editInlineMessageWithFileId(ctx, inlineMessageId, file_id, 'yt_' + formatString, videoDetails);

    } catch (error) {
        console.error(`${logPrefix} FAILED during YT download/cache for ${youtubeId} (InlineMsgID: ${inlineMessageId}):`, error);
        let userErrorKey = 'inline_cache_upload_failed'; let errorData = { error: error.message };
        if (error.message?.includes("Video is too long")) { userErrorKey = 'length_limit_error'; errorData = {}; }
        else if (error.message?.includes("Download service failed")) { userErrorKey = 'api_error_fetch'; errorData = { error: error.message }; }
        else if (error instanceof GrammyError && (error.error_code === 413 || error.description?.includes("too large"))) { userErrorKey = 'error_telegram_size'; errorData = {}; }
        else if (error instanceof GrammyError && error.description?.includes("wrong file identifier")) { userErrorKey = 'inline_cache_upload_failed'; errorData = { error: "Internal cache error."}; }
        else if (error instanceof GrammyError && (error.description?.includes("chat not found") || error.description?.includes("bot is not a participant"))) {
            console.error(`${logPrefix} CRITICAL: Cannot send YT to TARGET_CHANNEL_ID ${TARGET_CHANNEL_ID}. Check permissions/ID.`, error);
            userErrorKey = 'inline_error_general'; errorData = { error: "Bot configuration error." };
            await bot.api.sendMessage(BOT_ADMIN_ID, `🚨 CRITICAL ERROR: Cannot send YT cache file to channel ${TARGET_CHANNEL_ID}. Check bot permissions/ID. Error: ${error.description || error.message}`).catch(()=>{});
        }
        await setInlineError(userErrorKey, errorData);
    } finally {
        if (fileStreamResponse?.body?.cancel) fileStreamResponse.body.cancel().catch(e => console.warn(`${logPrefix} Error closing YT stream body via cancel():`, e));
        else if (fileStreamResponse?.body?.destroy) try { fileStreamResponse.body.destroy(); } catch (e) { console.warn(`${logPrefix} Error destroying YT stream body:`, e); }
        else if (fileStreamResponse?.body?.abort) try { fileStreamResponse.body.abort(); } catch (e) { console.warn(`${logPrefix} Error aborting YT stream body:`, e); }
        console.log(`${logPrefix} Finished YT processing request for ${youtubeId} (InlineMsgID: ${inlineMessageId}).`);
    }
}

async function processSpotifyDownloadAndCache(ctx, spotifyTrackId, inlineMessageId, cacheKey, metadata) {
    const userId = ctx.from?.id || ctx.chosenInlineResult?.from?.id || ctx.callbackQuery?.from?.id || 'N/A';
    const logPrefix = `[Spotify Download&Cache ${userId}]`;
    const trackUrl = `https://open.spotify.com/track/${spotifyTrackId}`;
    let trackStreamResponse = null;
    console.log(`${logPrefix} Starting Spotify process for ${spotifyTrackId}. Target: ${TARGET_CHANNEL_ID}, InlineMsgID: ${inlineMessageId}, CacheKey: ${cacheKey}`);

    if (!metadata) {
        console.error(`${logPrefix} Cannot process Spotify download/cache for ${inlineMessageId}: metadata missing.`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('spotify_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }); } catch (e) { }
        return;
    }

    const setInlineError = async (errorKey = 'inline_error_general', templateData = {}) => {
        console.error(`${logPrefix} Setting inline message ${inlineMessageId} to Spotify error state (${errorKey})`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t(errorKey, templateData), { reply_markup: undefined, parse_mode: "HTML" }); }
        catch (e) { if (!e.description?.includes("not found") && !e.description?.includes("can't be edited")) console.error(`${logPrefix} Failed to edit inline message ${inlineMessageId} to Spotify error state '${errorKey}':`, e.description || e); }
    };

    try {
        console.log(`${logPrefix} Requesting Spotify download stream for ${trackUrl}...`);
        trackStreamResponse = await downloadSpotifyTrack(trackUrl, null, { enableLogging: false });

        if (!trackStreamResponse || !trackStreamResponse.ok || !trackStreamResponse.body) {
            let errorDetail = `Status: ${trackStreamResponse?.status || 'N/A'}`;
            if (trackStreamResponse && !trackStreamResponse.ok) try { errorDetail += `, Body: ${(await trackStreamResponse.text()).substring(0, 100)}`; } catch { }
            throw new Error(`Spotify Download service failed. ${errorDetail}`);
        }
        console.log(`${logPrefix} Spotify stream obtained (Content-Type: ${trackStreamResponse.headers.get('content-type')}).`);

        const safeTitle = (metadata.name || ctx.t('fallback_track_title')).replace(/[/\\?%*:|"<>]/g, '-').substring(0, 100);
        const safeArtist = (metadata.artist || 'Unknown Artist').replace(/[/\\?%*:|"<>]/g, '-').substring(0, 50);
        const filename = `${safeArtist} - ${safeTitle}.mp3`;
        console.log(`${logPrefix} Using filename for Spotify channel upload: ${filename}`);

        const inputFile = new InputFile(trackStreamResponse.body, filename);
        const channelCaption = `Cache Spotify: ${spotifyTrackId}`;
        const sendOptions = {
            caption: channelCaption, disable_notification: true, parse_mode: undefined,
            title: metadata.name, performer: metadata.artist,
            thumbnail: metadata.cover_url ? new InputFile({ url: metadata.cover_url }) : undefined,
        };

        console.log(`${logPrefix} Sending Spotify audio to channel ${TARGET_CHANNEL_ID}...`);
        const sentMessage = await ctx.api.sendAudio(TARGET_CHANNEL_ID, inputFile, sendOptions);
        console.log(`${logPrefix} Successfully sent Spotify file to channel. Message ID: ${sentMessage.message_id}`);

        const file_id = sentMessage.audio?.file_id;
        if (!file_id) {
            throw new Error("Failed to get Spotify file_id after channel upload.");
        }
        console.log(`${logPrefix} Extracted Spotify file_id: ${file_id}`);

        fileIdCache[cacheKey] = file_id; await saveCache();
        console.log(`${logPrefix} Saved Spotify file_id ${file_id} to cache with key ${cacheKey}.`);

        await editInlineMessageWithFileId(ctx, inlineMessageId, file_id, 'spotify', metadata);

    } catch (error) {
        console.error(`${logPrefix} FAILED during Spotify download/cache process for ${spotifyTrackId} (InlineMsgID: ${inlineMessageId}):`, error);
        let userErrorKey = 'inline_cache_upload_failed'; let errorData = { error: error.message };
        if (error.message?.includes('Download service failed')) { userErrorKey = 'spotify_download_failed'; errorData = { error: error.message }; }
        else if (error instanceof GrammyError && (error.error_code === 413 || error.description?.includes("too large"))) { userErrorKey = 'error_telegram_size'; errorData = {}; }
        else if (error instanceof GrammyError && error.description?.includes("wrong file identifier")) { userErrorKey = 'inline_cache_upload_failed'; errorData = { error: "Internal cache error."}; }
        else if (error instanceof GrammyError && (error.description?.includes("chat not found") || error.description?.includes("bot is not a participant"))) {
            console.error(`${logPrefix} CRITICAL: Cannot send Spotify to TARGET_CHANNEL_ID ${TARGET_CHANNEL_ID}. Check permissions/ID.`, error);
            userErrorKey = 'inline_error_general'; errorData = { error: "Bot configuration error." };
            await bot.api.sendMessage(BOT_ADMIN_ID, `🚨 CRITICAL ERROR: Cannot send Spotify cache file to channel ${TARGET_CHANNEL_ID}. Check bot permissions/ID. Error: ${error.description || error.message}`).catch(()=>{});
        }
        await setInlineError(userErrorKey, errorData);
    } finally {
        if (trackStreamResponse?.body?.cancel) trackStreamResponse.body.cancel().catch(e => console.warn(`${logPrefix} Error closing Spotify stream body via cancel():`, e));
        else if (trackStreamResponse?.body?.destroy) try { trackStreamResponse.body.destroy(); } catch (e) { console.warn(`${logPrefix} Error destroying Spotify stream body:`, e); }
        else if (trackStreamResponse?.body?.abort) try { trackStreamResponse.body.abort(); } catch (e) { console.warn(`${logPrefix} Error aborting Spotify stream body:`, e); }
        console.log(`${logPrefix} Finished Spotify processing request for ${spotifyTrackId} (InlineMsgID: ${inlineMessageId}).`);
    }
}

async function processTikTokDownloadAndCache(ctx, tiktokVideoId, tiktokUrl, formatString, inlineMessageId, cacheKey, tiktokInfo) {
    const userId = ctx.from?.id || ctx.chosenInlineResult?.from?.id || ctx.callbackQuery?.from?.id || 'N/A';
    const logPrefix = `[TikTok Download&Cache ${userId}]`;
    let fileStreamResponse = null;
    console.log(`${logPrefix} Starting TikTok process for ${tiktokVideoId}, format: ${formatString}. Target: ${TARGET_CHANNEL_ID}, InlineMsgID: ${inlineMessageId}, CacheKey: ${cacheKey}`);

    if (!tiktokInfo) {
        console.error(`${logPrefix} Cannot process TikTok download/cache for ${inlineMessageId}: tiktokInfo missing.`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t('tiktok_metadata_failed'), { reply_markup: undefined, parse_mode: undefined }); } catch (e) { }
        return;
    }

    const setInlineError = async (errorKey = 'inline_error_general', templateData = {}) => {
        console.error(`${logPrefix} Setting inline message ${inlineMessageId} to TikTok error state (${errorKey})`);
        try { await ctx.api.editMessageTextInline(inlineMessageId, ctx.t(errorKey, templateData), { reply_markup: undefined, parse_mode: "HTML" }); }
        catch (e) { if (!e.description?.includes("not found") && !e.description?.includes("can't be edited")) console.error(`${logPrefix} Failed to edit inline message ${inlineMessageId} to TikTok error state '${errorKey}':`, e.description || e); }
    };

    try {
        console.log(`${logPrefix} Calling downloadTikTok(${tiktokUrl}, null, { format: ${formatString}, provider: 'auto' })`);
        fileStreamResponse = await downloadTikTok(tiktokUrl, null, { format: formatString, provider: 'auto', enableLogging: false });
        console.log(`${logPrefix} Received response from downloadTikTok. Status: ${fileStreamResponse?.status}`);

        if (!fileStreamResponse || !fileStreamResponse.body || !fileStreamResponse.ok) {
             let apiErrorMsg = `Status: ${fileStreamResponse?.status || 'N/A'}`;
             if (fileStreamResponse && !fileStreamResponse.ok) try { apiErrorMsg += `, Body: ${(await fileStreamResponse.text()).substring(0, 100)}`; } catch { }
            throw new Error(`TikTok Download service failed. ${apiErrorMsg}`);
        }

        console.log(`${logPrefix} TikTok File stream obtained. Sending to channel ${TARGET_CHANNEL_ID}...`);
        const safeTitle = (tiktokInfo.description || `tiktok_${tiktokVideoId}`).replace(/[/\\?%*:|"<>]/g, '-').substring(0, 100);
        const filename = `${safeTitle}.${formatString}`;
        console.log(`${logPrefix} Using filename for TikTok channel upload: ${filename}`);

        const inputFile = new InputFile(fileStreamResponse.body, filename);
        const channelCaption = `Cache TikTok: ${tiktokVideoId} | ${formatString}`;
        const sendOptions = { caption: channelCaption, disable_notification: true, parse_mode: undefined };

        let sentMessage;
        console.log(`${logPrefix} Sending TikTok ${formatString} to channel...`);
        if (formatString === 'mp3') {
            sentMessage = await ctx.api.sendAudio(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, title: safeTitle, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
        } else {
            sentMessage = await ctx.api.sendVideo(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, supports_streaming: true, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
        }
        console.log(`${logPrefix} Successfully sent TikTok file to channel ${TARGET_CHANNEL_ID}. Message ID: ${sentMessage.message_id}`);

        const file_id = formatString === 'mp3' ? sentMessage.audio?.file_id : sentMessage.video?.file_id;
        if (!file_id) throw new Error("Failed to get TikTok file_id after channel upload.");
        console.log(`${logPrefix} Extracted TikTok file_id: ${file_id}`);

        fileIdCache[cacheKey] = file_id; await saveCache();
        console.log(`${logPrefix} Saved TikTok file_id ${file_id} to cache with key ${cacheKey}.`);

        await editInlineMessageWithFileId(ctx, inlineMessageId, file_id, 'tk_' + formatString, tiktokInfo);

    } catch (error) {
        console.error(`${logPrefix} FAILED during TikTok download/cache for ${tiktokVideoId} (InlineMsgID: ${inlineMessageId}):`, error);
        let userErrorKey = 'inline_cache_upload_failed'; let errorData = { error: error.message };
        if (error.message?.includes("Download service failed")) { userErrorKey = 'tiktok_download_failed'; errorData = { error: error.message }; }
        else if (error instanceof GrammyError && (error.error_code === 413 || error.description?.includes("too large"))) { userErrorKey = 'error_telegram_size'; errorData = {}; }
        else if (error instanceof GrammyError && error.description?.includes("wrong file identifier")) { userErrorKey = 'inline_cache_upload_failed'; errorData = { error: "Internal cache error."}; }
        else if (error instanceof GrammyError && (error.description?.includes("chat not found") || error.description?.includes("bot is not a participant"))) {
            console.error(`${logPrefix} CRITICAL: Cannot send TikTok to TARGET_CHANNEL_ID ${TARGET_CHANNEL_ID}. Check permissions/ID.`, error);
            userErrorKey = 'inline_error_general'; errorData = { error: "Bot configuration error." };
            await bot.api.sendMessage(BOT_ADMIN_ID, `🚨 CRITICAL ERROR: Cannot send TikTok cache file to channel ${TARGET_CHANNEL_ID} during normal download. Check permissions/ID. Error: ${error.description || error.message}`).catch(()=>{});
        }
        await setInlineError(userErrorKey, errorData);
    } finally {
        if (fileStreamResponse?.body?.cancel) fileStreamResponse.body.cancel().catch(e => console.warn(`${logPrefix} Error closing TikTok stream body via cancel():`, e));
        else if (fileStreamResponse?.body?.destroy) try { fileStreamResponse.body.destroy(); } catch (e) { console.warn(`${logPrefix} Error destroying TikTok stream body:`, e); }
        else if (fileStreamResponse?.body?.abort) try { fileStreamResponse.body.abort(); } catch (e) { console.warn(`${logPrefix} Error aborting TikTok stream body:`, e); }
        console.log(`${logPrefix} Finished TikTok processing request for ${tiktokVideoId} (InlineMsgID: ${inlineMessageId}).`);
    }
}


async function processYouTubeDownloadRequestNormalWithCache(ctx, youtubeId, youtubeUrl, formatString, chosenQualityString, editTarget) {
    const userId = ctx.from?.id || 'N/A';
    const lang = ctx.lang || defaultLocale;
    const logPrefix = `[ProcessDL YT Normal ${userId}]`;
    const targetId = `${editTarget.chatId}/${editTarget.messageId}`;
    const cacheKey = `yt:${youtubeId}:${formatString}:${chosenQualityString}`;

    console.log(`${logPrefix} Starting YT process for ${youtubeId}, format: ${formatString}, quality: ${chosenQualityString}. Target: ${targetId}. Cache key: ${cacheKey}`);

    let statusMessageExists = true;

    const editStatus = async (textKey, templateData = {}, extra = {}) => {
        if (!statusMessageExists) return;
        try {
            await ctx.api.editMessageText(editTarget.chatId, editTarget.messageId, ctx.t(textKey, templateData), { parse_mode: undefined, ...extra });
        } catch (e) {
            if (e.description?.includes("not found")) { statusMessageExists = false; console.warn(`${logPrefix} Status message ${targetId} not found during edit.`); }
            else if (!e.description?.includes("modified")) { console.warn(`${logPrefix} Failed to edit status msg ${targetId} to "${ctx.t(textKey, templateData).substring(0,50)}...":`, e.description || e); }
        }
    };

    const sendErrorReply = async (translationKey, templateData = {}) => {
        const suffix = BOT_USERNAME_SUFFIX();
        const errorMessage = ctx.t(translationKey, templateData) + suffix;
        console.error(`${logPrefix} Sending YT error to ${userId} (target: ${targetId}): ${ctx.t(translationKey, templateData)}`);
        try {
             if (statusMessageExists) {
                await ctx.api.editMessageText(editTarget.chatId, editTarget.messageId, errorMessage, { ...replyOpts(), reply_markup: undefined });
                statusMessageExists = false;
             } else {
                 await ctx.reply(errorMessage, replyOpts()).catch(replyErr => console.error(`${logPrefix} Failed even to send new YT error reply:`, replyErr));
             }
        } catch (e) {
             console.error(`${logPrefix} Failed to edit YT message ${targetId} with error '${translationKey}':`, e.description || e);
             if (ctx.chat?.id && !e.description?.includes("not found")) {
                  await ctx.reply(errorMessage, replyOpts()).catch(replyErr => console.error(`${logPrefix} Failed fallback YT error reply:`, replyErr));
             }
             statusMessageExists = false;
        }
    };

    const sendFileToUser = async (file_id, videoDetails) => {
        if (!videoDetails) {
            console.error(`${logPrefix} Cannot send YT file ${file_id}: videoDetails missing.`);
            await sendErrorReply('error_fetching_title'); return;
        }
        try {
            if (statusMessageExists) {
                try { await ctx.api.deleteMessage(editTarget.chat.id, editTarget.message_id); statusMessageExists = false; console.log(`${logPrefix} Deleted status message ${targetId}.`); }
                catch (delErr) { if (delErr.description?.includes("not found")) statusMessageExists = false; else { console.warn(`${logPrefix} Could not delete status msg ${targetId}:`, delErr.description || delErr); statusMessageExists = false; } }
            }

            const videoTitle = videoDetails.title || ctx.t('fallback_video_title');
            const caption = `${videoTitle}\n${youtubeUrl}${BOT_USERNAME_SUFFIX()}`;
            const baseOptions = { caption: caption, ...replyOpts() };

            if (formatString === 'mp3') {
                await ctx.replyWithAudio(file_id, { ...baseOptions, duration: videoDetails.seconds, performer: videoDetails.author?.name, title: videoDetails.title });
            } else {
                await ctx.replyWithVideo(file_id, { ...baseOptions, duration: videoDetails.seconds, supports_streaming: true });
            }
            console.log(`${logPrefix} Successfully sent YT file ${file_id} for ${youtubeId} to user ${userId}.`);

        } catch (telegramError) {
            console.error(`${logPrefix} Telegram send YT file_id error for ${youtubeId} (FileID: ${file_id}):`, telegramError.description || telegramError);
            let errorKey = 'general_error'; let errorData = { error: `Send failed: ${telegramError.description || telegramError.message}` };
            if (telegramError instanceof GrammyError) {
                 if (telegramError.error_code === 400 && telegramError.description?.includes("wrong file identifier")) {
                     errorKey = 'inline_cache_upload_failed'; errorData = { error: 'Invalid cached file.'};
                     console.warn(`${logPrefix} Removing invalid YT file_id ${file_id} from cache for key ${cacheKey}.`);
                     delete fileIdCache[cacheKey]; await saveCache();
                 } else if (telegramError.error_code === 413 || telegramError.description?.includes("too large")) { errorKey = 'error_telegram_size'; errorData = {}; }
                 else if (telegramError.description?.includes("INPUT_USER_DEACTIVATED") || telegramError.description?.includes("BOT_IS_BLOCKED") || telegramError.description?.includes("USER_IS_BLOCKED")) { console.warn(`${logPrefix} User ${userId} interaction blocked (${telegramError.description}).`); return; }
            }
            await sendErrorReply(errorKey, errorData);
        }
    };

    try {
        const videoDetails = await getVideoDetailsSafe(youtubeId, lang);
        if (!videoDetails) { await sendErrorReply('error_fetching_title'); return; }

        const cachedFileId = fileIdCache[cacheKey];
        if (cachedFileId) {
            console.log(`${logPrefix} Cache HIT for YT ${cacheKey}. Sending directly.`);
            await editStatus('sending_file');
            await sendFileToUser(cachedFileId, videoDetails);
            return;
        }

        console.log(`${logPrefix} Cache MISS for YT ${cacheKey}. Starting download & cache process.`);
        const qualityDisplayName = getQualityDisplay(ctx, chosenQualityString);
        const formatDisplayString = formatString.toUpperCase();
        await editStatus('processing_detailed', { format: formatDisplayString, quality: qualityDisplayName });
        await editStatus('requesting_download');
        await ctx.replyWithChatAction(formatString === 'mp3' ? 'upload_audio' : 'upload_video').catch(()=>{});

        let fileStreamResponse = null;

        try {
            console.log(`${logPrefix} Calling downloadYouTubeVideo(${youtubeUrl}, ${formatString}, ${chosenQualityString})`);
            fileStreamResponse = await downloadYouTubeVideo(youtubeUrl, formatString, chosenQualityString, null, { enableLogging: true });
            console.log(`${logPrefix} Received response from downloadYouTubeVideo. Status: ${fileStreamResponse?.status}`);

            if (!fileStreamResponse || !fileStreamResponse.body || !fileStreamResponse.ok) {
                 let apiErrorMsg = `Status: ${fileStreamResponse?.status || 'N/A'}`;
                 if (fileStreamResponse && !fileStreamResponse.ok) try { apiErrorMsg += `, Body: ${(await fileStreamResponse.text()).substring(0,100)}`; } catch {}
                if (apiErrorMsg.includes("Video is too long")) throw new Error("Video is too long");
                throw new Error(`YT Download service failed. ${apiErrorMsg}`);
            }

            console.log(`${logPrefix} YT File stream obtained. Sending to channel ${TARGET_CHANNEL_ID}...`);
            let filename = `${(videoDetails.title || youtubeId).substring(0, 100)}_${formatString}_${chosenQualityString}.${formatString}`;
            const contentDisposition = fileStreamResponse.headers.get('content-disposition');
             if (contentDisposition) {
                const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i); if (utf8Match?.[1]) try {filename = decodeURIComponent(utf8Match[1]);} catch {}
                else { const asciiMatch = contentDisposition.match(/filename="?([^";]+)"?/i); if(asciiMatch?.[1]) filename = asciiMatch[1];}
             }
             filename = filename.replace(/[/\\?%*:|"<>]/g, '-').substring(0, 200);
            console.log(`${logPrefix} Using filename for YT channel upload: ${filename}`);

            const inputFile = new InputFile(fileStreamResponse.body, filename);
            const channelCaption = `Cache YT: ${youtubeId} | ${formatString} | ${chosenQualityString}`;
            const sendOptions = { caption: channelCaption, disable_notification: true, parse_mode: undefined };

            let sentMessage;
            console.log(`${logPrefix} Sending YT ${formatString} to channel...`);
            if (formatString === 'mp3') {
                sentMessage = await ctx.api.sendAudio(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, duration: videoDetails.seconds, performer: videoDetails.author?.name, title: videoDetails.title });
            } else {
                sentMessage = await ctx.api.sendVideo(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, duration: videoDetails.seconds, supports_streaming: true, thumbnail: videoDetails.thumbnail ? new InputFile({ url: videoDetails.thumbnail }) : undefined });
            }
            console.log(`${logPrefix} Successfully sent YT file to channel. Message ID: ${sentMessage.message_id}`);

            const new_file_id = formatString === 'mp3' ? sentMessage.audio?.file_id : sentMessage.video?.file_id;
            if (!new_file_id) throw new Error("Failed to get YT file_id after channel upload.");
            console.log(`${logPrefix} Extracted YT file_id: ${new_file_id}`);
            fileIdCache[cacheKey] = new_file_id; await saveCache();
            console.log(`${logPrefix} Saved YT file_id ${new_file_id} to cache with key ${cacheKey}.`);

            await editStatus('sending_file');
            await sendFileToUser(new_file_id, videoDetails);

        } catch (error) {
            console.error(`${logPrefix} FAILED during YT download/cache for ${youtubeId} (Target: ${targetId}):`, error);
            let userErrorKey = 'inline_cache_upload_failed'; let errorData = { error: error.message };
             if (error.message?.includes("Video is too long")) { userErrorKey = 'length_limit_error'; errorData = {}; }
             else if (error.message?.includes("Download service failed")) { userErrorKey = 'api_error_fetch'; errorData = { error: error.message }; }
             else if (error instanceof GrammyError && (error.error_code === 413 || error.description?.includes("too large"))) { userErrorKey = 'error_telegram_size'; errorData = {}; }
             else if (error instanceof GrammyError && (error.description?.includes("chat not found") || error.description?.includes("bot is not a participant"))) {
                 console.error(`${logPrefix} CRITICAL: Cannot send YT to TARGET_CHANNEL_ID ${TARGET_CHANNEL_ID}. Check permissions/ID.`, error);
                 userErrorKey = 'general_error'; errorData = { error: "Bot configuration error." };
                 await bot.api.sendMessage(BOT_ADMIN_ID, `🚨 CRITICAL ERROR: Cannot send YT cache file to channel ${TARGET_CHANNEL_ID} during normal download. Check permissions/ID. Error: ${error.description || error.message}`).catch(()=>{});
             }
            await sendErrorReply(userErrorKey, errorData);
        } finally {
             if (fileStreamResponse?.body?.cancel) fileStreamResponse.body.cancel().catch(e => console.warn(`${logPrefix} Error closing YT stream body via cancel():`, e));
             else if (fileStreamResponse?.body?.destroy) try { fileStreamResponse.body.destroy(); } catch (e) { console.warn(`${logPrefix} Error destroying YT stream body:`, e); }
             else if (fileStreamResponse?.body?.abort) try { fileStreamResponse.body.abort(); } catch (e) { console.warn(`${logPrefix} Error aborting YT stream body:`, e); }
        }

    } catch (error) {
        console.error(`${logPrefix} UNEXPECTED error before YT download for ${youtubeId} (Target: ${targetId}):`, error);
        await sendErrorReply('general_error', { error: error.message });
    } finally {
        console.log(`${logPrefix} Finished YT processing request for ${youtubeId} (Target: ${targetId}).`);
    }
}

async function processTikTokDownloadRequestNormalWithCache(ctx, tiktokVideoId, tiktokUrl, formatString, editTarget) {
    const userId = ctx.from?.id || 'N/A';
    const lang = ctx.lang || defaultLocale;
    const logPrefix = `[ProcessDL TikTok Normal ${userId}]`;
    const targetId = `${editTarget.chatId}/${editTarget.messageId}`;
    const cacheKey = `tk:${tiktokVideoId}:${formatString}`;

    console.log(`${logPrefix} Starting TikTok process for ${tiktokVideoId}, format: ${formatString}. Target: ${targetId}. Cache key: ${cacheKey}`);

    let statusMessageExists = true;

    const editStatus = async (textKey, templateData = {}, extra = {}) => {
        if (!statusMessageExists) return;
        try {
            await ctx.api.editMessageText(editTarget.chatId, editTarget.messageId, ctx.t(textKey, templateData), { parse_mode: undefined, ...extra });
        } catch (e) {
            if (e.description?.includes("not found")) { statusMessageExists = false; console.warn(`${logPrefix} Status message ${targetId} not found during edit.`); }
            else if (!e.description?.includes("modified")) { console.warn(`${logPrefix} Failed to edit status msg ${targetId} to "${ctx.t(textKey, templateData).substring(0,50)}...":`, e.description || e); }
        }
    };

    const sendErrorReply = async (translationKey, templateData = {}) => {
        const suffix = BOT_USERNAME_SUFFIX();
        const errorMessage = ctx.t(translationKey, templateData) + suffix;
        console.error(`${logPrefix} Sending TikTok error to ${userId} (target: ${targetId}): ${ctx.t(translationKey, templateData)}`);
        try {
             if (statusMessageExists) {
                await ctx.api.editMessageText(editTarget.chatId, editTarget.messageId, errorMessage, { ...replyOpts(), reply_markup: undefined });
                statusMessageExists = false;
             } else {
                 await ctx.reply(errorMessage, replyOpts()).catch(replyErr => console.error(`${logPrefix} Failed even to send new TikTok error reply:`, replyErr));
             }
        } catch (e) {
             console.error(`${logPrefix} Failed to edit TikTok message ${targetId} with error '${translationKey}':`, e.description || e);
             if (ctx.chat?.id && !e.description?.includes("not found")) {
                  await ctx.reply(errorMessage, replyOpts()).catch(replyErr => console.error(`${logPrefix} Failed fallback TikTok error reply:`, replyErr));
             }
             statusMessageExists = false;
        }
    };

    const sendFileToUser = async (file_id, tiktokInfo) => {
        if (!tiktokInfo) {
            console.error(`${logPrefix} Cannot send TikTok file ${file_id}: tiktokInfo missing.`);
            await sendErrorReply('tiktok_metadata_failed'); return;
        }
        try {
            if (statusMessageExists) {
                try { await ctx.api.deleteMessage(editTarget.chat.id, editTarget.message_id); statusMessageExists = false; console.log(`${logPrefix} Deleted status message ${targetId}.`); }
                catch (delErr) { if (delErr.description?.includes("not found")) statusMessageExists = false; else { console.warn(`${logPrefix} Could not delete status msg ${targetId}:`, delErr.description || delErr); statusMessageExists = false; } }
            }

            const videoTitle = tiktokInfo.description?.substring(0, 150) || ctx.t('fallback_tiktok_title');
            const displayUrl = tiktokUrl || `https://www.tiktok.com/video/${tiktokVideoId}`;
            const caption = `${videoTitle}\n${displayUrl}${BOT_USERNAME_SUFFIX()}`;
            const baseOptions = { caption: caption, ...replyOpts() };

            if (formatString === 'mp3') {
                await ctx.replyWithAudio(file_id, { ...baseOptions, title: videoTitle, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
            } else {
                await ctx.replyWithVideo(file_id, { ...baseOptions, supports_streaming: true, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
            }
            console.log(`${logPrefix} Successfully sent TikTok file ${file_id} for ${tiktokVideoId} to user ${userId}.`);

        } catch (telegramError) {
            console.error(`${logPrefix} Telegram send TikTok file_id error for ${tiktokVideoId} (FileID: ${file_id}):`, telegramError.description || telegramError);
            let errorKey = 'general_error'; let errorData = { error: `Send failed: ${telegramError.description || telegramError.message}` };
            if (telegramError instanceof GrammyError) {
                 if (telegramError.error_code === 400 && telegramError.description?.includes("wrong file identifier")) {
                     errorKey = 'inline_cache_upload_failed'; errorData = { error: 'Invalid cached file.'};
                     console.warn(`${logPrefix} Removing invalid TikTok file_id ${file_id} from cache for key ${cacheKey}.`);
                     delete fileIdCache[cacheKey]; await saveCache();
                 } else if (telegramError.error_code === 413 || telegramError.description?.includes("too large")) { errorKey = 'error_telegram_size'; errorData = {}; }
                 else if (telegramError.description?.includes("INPUT_USER_DEACTIVATED") || telegramError.description?.includes("BOT_IS_BLOCKED") || telegramError.description?.includes("USER_IS_BLOCKED")) { console.warn(`${logPrefix} User ${userId} interaction blocked (${telegramError.description}).`); return; }
            }
            await sendErrorReply(errorKey, errorData);
        }
    };

    try {
        const tiktokInfo = await getTikTokDetailsSafe(tiktokUrl, lang);
        if (!tiktokInfo) { await sendErrorReply('tiktok_metadata_failed'); return; }

        const cachedFileId = fileIdCache[cacheKey];
        if (cachedFileId) {
            console.log(`${logPrefix} Cache HIT for TikTok ${cacheKey}. Sending directly.`);
            await editStatus('sending_file');
            await sendFileToUser(cachedFileId, tiktokInfo);
            return;
        }

        console.log(`${logPrefix} Cache MISS for TikTok ${cacheKey}. Starting download & cache process.`);
        const formatDisplayString = formatString.toUpperCase();
        await editStatus('processing_tiktok', { format: formatDisplayString });
        await editStatus('requesting_download');
        await ctx.replyWithChatAction(formatString === 'mp3' ? 'upload_audio' : 'upload_video').catch(()=>{});

        let fileStreamResponse = null;

        try {
            console.log(`${logPrefix} Calling downloadTikTok(${tiktokUrl}, null, { format: ${formatString}, provider: 'auto' })`);
            fileStreamResponse = await downloadTikTok(tiktokUrl, null, { format: formatString, provider: 'auto', enableLogging: false });
            console.log(`${logPrefix} Received response from downloadTikTok. Status: ${fileStreamResponse?.status}`);

            if (!fileStreamResponse || !fileStreamResponse.body || !fileStreamResponse.ok) {
                 let apiErrorMsg = `Status: ${fileStreamResponse?.status || 'N/A'}`;
                 if (fileStreamResponse && !fileStreamResponse.ok) try { apiErrorMsg += `, Body: ${(await fileStreamResponse.text()).substring(0,100)}`; } catch {}
                throw new Error(`TikTok Download service failed. ${apiErrorMsg}`);
            }

            console.log(`${logPrefix} TikTok File stream obtained. Sending to channel ${TARGET_CHANNEL_ID}...`);
            const safeTitle = (tiktokInfo.description || `tiktok_${tiktokVideoId}`).replace(/[/\\?%*:|"<>]/g, '-').substring(0, 100);
            const filename = `${safeTitle}.${formatString}`;
            console.log(`${logPrefix} Using filename for TikTok channel upload: ${filename}`);

            const inputFile = new InputFile(fileStreamResponse.body, filename);
            const channelCaption = `Cache TikTok: ${tiktokVideoId} | ${formatString}`;
            const sendOptions = { caption: channelCaption, disable_notification: true, parse_mode: undefined };

            let sentMessage;
            console.log(`${logPrefix} Sending TikTok ${formatString} to channel...`);
            if (formatString === 'mp3') {
                sentMessage = await ctx.api.sendAudio(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, title: safeTitle, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
            } else {
                sentMessage = await ctx.api.sendVideo(TARGET_CHANNEL_ID, inputFile, { ...sendOptions, supports_streaming: true, thumbnail: tiktokInfo.thumbnailUrl ? new InputFile({ url: tiktokInfo.thumbnailUrl }) : undefined });
            }
            console.log(`${logPrefix} Successfully sent TikTok file to channel ${TARGET_CHANNEL_ID}. Message ID: ${sentMessage.message_id}`);

            const new_file_id = formatString === 'mp3' ? sentMessage.audio?.file_id : sentMessage.video?.file_id;
            if (!new_file_id) throw new Error("Failed to get TikTok file_id after channel upload.");
            console.log(`${logPrefix} Extracted TikTok file_id: ${new_file_id}`);
            fileIdCache[cacheKey] = new_file_id; await saveCache();
            console.log(`${logPrefix} Saved TikTok file_id ${new_file_id} to cache with key ${cacheKey}.`);

            await editStatus('sending_file');
            await sendFileToUser(new_file_id, tiktokInfo);

        } catch (error) {
            console.error(`${logPrefix} FAILED during TikTok download/cache for ${tiktokVideoId} (Target: ${targetId}):`, error);
            let userErrorKey = 'inline_cache_upload_failed'; let errorData = { error: error.message };
             if (error.message?.includes("Download service failed")) { userErrorKey = 'tiktok_download_failed'; errorData = { error: error.message }; }
             else if (error instanceof GrammyError && (error.error_code === 413 || error.description?.includes("too large"))) { userErrorKey = 'error_telegram_size'; errorData = {}; }
             else if (error instanceof GrammyError && error.description?.includes("wrong file identifier")) { userErrorKey = 'inline_cache_upload_failed'; errorData = { error: "Internal cache error."}; }
             else if (error instanceof GrammyError && (error.description?.includes("chat not found") || error.description?.includes("bot is not a participant"))) {
                 console.error(`${logPrefix} CRITICAL: Cannot send TikTok to TARGET_CHANNEL_ID ${TARGET_CHANNEL_ID}. Check permissions/ID.`, error);
                 userErrorKey = 'general_error'; errorData = { error: "Bot configuration error." };
                 await bot.api.sendMessage(BOT_ADMIN_ID, `🚨 CRITICAL ERROR: Cannot send TikTok cache file to channel ${TARGET_CHANNEL_ID} during normal download. Check permissions/ID. Error: ${error.description || error.message}`).catch(()=>{});
             }
            await sendErrorReply(userErrorKey, errorData);
        } finally {
             if (fileStreamResponse?.body?.cancel) fileStreamResponse.body.cancel().catch(e => console.warn(`${logPrefix} Error closing TikTok stream body via cancel():`, e));
             else if (fileStreamResponse?.body?.destroy) try { fileStreamResponse.body.destroy(); } catch (e) { console.warn(`${logPrefix} Error destroying TikTok stream body:`, e); }
             else if (fileStreamResponse?.body?.abort) try { fileStreamResponse.body.abort(); } catch (e) { console.warn(`${logPrefix} Error aborting TikTok stream body:`, e); }
        }

    } catch (error) {
        console.error(`${logPrefix} UNEXPECTED error before TikTok download for ${tiktokVideoId} (Target: ${targetId}):`, error);
        if (error.message?.includes('getTikTokDetailsSafe')) {
             await sendErrorReply('tiktok_metadata_failed', { error: error.message });
        } else {
             await sendErrorReply('general_error', { error: error.message });
        }
    } finally {
        console.log(`${logPrefix} Finished TikTok processing request for ${tiktokVideoId} (Target: ${targetId}).`);
    }
}


bot.catch((err) => {
    const ctx = err.ctx;
    const updateId = ctx?.update?.update_id || 'N/A';
    const updateType = ctx?.updateType || 'N/A';
    const userId = ctx?.from?.id || ctx?.inlineQuery?.from?.id || ctx?.chosenInlineResult?.from?.id || ctx?.callbackQuery?.from?.id || 'N/A';
    const lang = ctx?.lang || userLanguages[userId] || defaultLocale;
    const e = err.error;

    console.error(`💥 Unhandled error caught! Update ID: ${updateId}, Type: ${updateType}, User: ${userId}`);

    if (e instanceof GrammyError) {
        console.error(`[bot.catch] GrammyError: ${e.description}`, e.payload ? `Payload: ${JSON.stringify(e.payload)}` : '');
        if (e.description.includes("message is not modified") || e.description.includes("query is too old") || e.description.includes("message to edit not found") || e.description.includes("message can't be edited") || e.description.includes("bot was blocked") || e.description.includes("user is deactivated") || e.description.includes("chat not found") || e.description.includes("inline message ID is invalid") || e.description.includes("QUERY_ID_INVALID")) {
            console.log(`[bot.catch] (Ignoring common/expected GrammyError: ${e.description})`); return;
        }
    } else if (e instanceof HttpError) { console.error("[bot.catch] HttpError (Telegram connection):", e); }
    else if (e?.name?.startsWith('Sequelize')) { console.error(`[bot.catch] Sequelize Error (${e.name}):`, e.message, e.parent ? `\n  Parent: ${e.parent.message}` : ''); bot.api.sendMessage(BOT_ADMIN_ID, `🚨 DATABASE ERROR: ${e.name} - ${e.message}`).catch(()=>{}); }
    else if (e?.message?.toLowerCase().includes('spotify')) { console.error("[bot.catch] Spotify Library/API Error:", e.message); }
    else if (e?.message?.toLowerCase().includes('tiktok')) { console.error("[bot.catch] TikTok Library/API Error:", e.message); }
    else if (e?.message?.toLowerCase().includes('youtube') || e?.message?.toLowerCase().includes('cnvmp3')) { console.error("[bot.catch] YouTube Library/API Error:", e.message); }
    else { console.error("[bot.catch] Unknown or Library error:", e?.message || e); if (e?.stack) console.error(e.stack); }

    const errorMessage = t(lang, 'error_occurred_try_again');
    const errorMessageInline = t(lang, 'inline_error_general');
    const suffix = BOT_USERNAME_SUFFIX();

    try {
        if (updateType === 'inline_query' && ctx.inlineQuery) {
            ctx.answerInlineQuery([{ type: 'article', id: 'bot_error', title: errorMessageInline, input_message_content: { message_text: errorMessageInline, parse_mode: undefined} }], {cache_time: 5}).catch(()=>{});
        } else if ((updateType === 'chosen_inline_result' || updateType === 'callback_query') && (ctx.chosenInlineResult?.inline_message_id || ctx.callbackQuery?.inline_message_id)) {
             const inlineMsgId = ctx.chosenInlineResult?.inline_message_id || ctx.callbackQuery?.inline_message_id;
             if (inlineMsgId) {
                 ctx.api.editMessageTextInline(inlineMsgId, errorMessageInline, { reply_markup: undefined, parse_mode: undefined }).catch(inlineEditErr => { if (!inlineEditErr.description?.includes("not found") && !inlineEditErr.description?.includes("not modified") && !inlineEditErr.description?.includes("invalid")) console.error(`[bot.catch] Failed to edit inline message ${inlineMsgId} with error:`, inlineEditErr.description || inlineEditErr); });
             } else { console.error(`[bot.catch] Error during ${updateType} for user ${userId}, but inline_message_id missing.`); }
        }
        else if (updateType === 'callback_query' && ctx.chat?.id && ctx.callbackQuery?.message?.message_id) {
             const targetChatId = ctx.chat.id; const targetMessageId = ctx.callbackQuery.message.message_id;
             ctx.api.editMessageText(targetChatId, targetMessageId, errorMessage + suffix, { ...replyOpts(), reply_markup: undefined }).catch(editErr => { if (!editErr.description?.includes("not found") && !editErr.description?.includes("not modified")) ctx.reply(errorMessage + suffix, replyOpts()).catch(replyErr => console.error("☠️ [bot.catch] Failed fallback error reply:", replyErr.description || replyErr)); });
        }
        else if (ctx.chat?.id) {
            ctx.reply(errorMessage + suffix, replyOpts()).catch(replyErr => console.error("☠️ [bot.catch] Failed to send error reply:", replyErr.description || replyErr));
        }
        else { console.error(`☠️ [bot.catch] Cannot send error notification: No chat context found for update type ${updateType}.`); }
    } catch (notifyError) { console.error(`☠️ [bot.catch] Error while trying to notify user about the original error:`, notifyError); }
});


let expressServer;
async function startBot() {
    try {
        console.log("💾 Loading File ID Cache..."); await loadCache();
        console.log("🗣️ Loading User Language Cache..."); await loadLangCache();
        console.log("🚀 Initializing Database connection and models..."); await initializeDatabase();

        const app = express();
        app.get('/status', (req, res) => { res.status(200).send('ok'); });
        app.get('/', (req, res) => { res.send(`Bot is running. Health check at /status. Admin: ${BOT_ADMIN_ID}`); });
        expressServer = app.listen(EXPRESS_PORT, () => { console.log(`🩺 Health check server listening on port ${EXPRESS_PORT}`); });
        expressServer.on('error', (err) => {
             console.error(`❌ Express server error on port ${EXPRESS_PORT}:`, err);
             if (err.code === 'EADDRINUSE') { console.error(`Port ${EXPRESS_PORT} is already in use. Shutting down.`); shutdown('EADDRINUSE').catch(() => process.exit(1)); }
         });

        console.log("🔧 Setting bot commands...");
        await bot.api.setMyCommands([
            { command: "start", description: "Start the bot / Select language" },
            { command: "stats", description: "Show bot usage statistics (Admin)" }
        ]);

        botInfo = await bot.api.getMe();
        console.log(`🤖 Starting bot @${botInfo.username} (ID: ${botInfo.id})...`);

        run(bot);
        console.log(`✅ Bot runner started.`);
        console.log(`🔑 Admin user ID for /stats: ${BOT_ADMIN_ID}`);
        console.log(`📢 Target channel ID for caching: ${TARGET_CHANNEL_ID}`);
        console.log("✨ Inline mode is enabled. Ensure it's enabled in @BotFather too!");
        console.log(`🔗 Bot link: https://t.me/${botInfo.username}`);
        console.log("🚀 Supported services: YouTube, Spotify, TikTok");

    } catch (err) {
        console.error("❌ FATAL ERROR during bot startup:", err);
        await shutdown('STARTUP_FAILURE').catch(() => process.exit(1));
        process.exit(1);
    }
}

const shutdown = async (signal) => {
    console.log(`\n🚦 ${signal} received. Initiating graceful shutdown...`);
    let exitCode = (signal === 'SIGINT' || signal === 'SIGTERM') ? 0 : 1;
    if (signal === 'EADDRINUSE' || signal === 'STARTUP_FAILURE') exitCode = 1;

    console.log("⏳ Stopping bot runner (will happen on process exit)...");

    const cachePromises = [
        saveCache().then(() => console.log("💾 File ID Cache saved.")).catch(e => console.error("⚠️ Error saving File ID Cache:", e)),
        saveLangCache().then(() => console.log("🗣️ User Language Cache saved.")).catch(e => console.error("⚠️ Error saving Lang Cache:", e))
    ];

    const expressClosePromise = new Promise((resolve) => {
        if (expressServer) {
            console.log("🔌 Closing Express server...");
            expressServer.close((err) => { if (err) { console.error("⚠️ Error closing Express server:", err); exitCode = 1; } else { console.log("🔌 Express server closed."); } resolve(); });
        } else { resolve(); }
    });

    const dbClosePromise = new Promise(async (resolve) => {
        try { if (sequelize?.close) { console.log("🛢️ Closing database connection..."); await sequelize.close(); console.log("✅ Database connection closed."); } }
        catch (e) { console.error("⚠️ Error closing database connection:", e); exitCode = 1; }
        finally { resolve(); }
    });

    await Promise.all([...cachePromises, expressClosePromise, dbClosePromise]);

    console.log(`🏁 Shutdown complete. Exiting with code ${exitCode}.`);
    setTimeout(() => { console.warn("⏰ Forcing exit after timeout."); process.exit(exitCode); }, 3000);
    process.exit(exitCode);
};

process.once('SIGINT', () => shutdown('SIGINT'));
process.once('SIGTERM', () => shutdown('SIGTERM'));

startBot();
