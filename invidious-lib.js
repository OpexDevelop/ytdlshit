import fs from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { getYouTubeVideoId } from 'opex-yt-id';
import PQueue from 'p-queue';

// --- КОНФИГУРАЦИЯ ---
const LOG_PREFIX = '[InvidiousDL]';
const PRIMARY_INSTANCE = 'https://inv.perditum.com';
const INSTANCES_API = 'https://api.invidious.io/instances.json?sort_by=health';

// Глобальная очередь скачивания: строго 1 за раз
const downloadQueue = new PQueue({ concurrency: 1 });

// --- ВНУТРЕННИЙ КЛАСС СКРЕЙПЕРА ---
class InvidiousScraper {
    constructor(enableLogging = false) {
        this.enableLogging = enableLogging;
        this.instances = [];
    }

    log(msg) {
        if (this.enableLogging) console.log(`${LOG_PREFIX} ${msg}`);
    }

    async fetchInstances() {
        this.log('Fetching instances list...');
        try {
            const res = await fetch(INSTANCES_API);
            const data = await res.json();
            
            const publicInstances = data
                .filter(item => item[1].type === 'https' && item[1].monitor && item[1].monitor.uptime > 90)
                .map(item => item[1].uri.replace(/\/$/, ''));

            const others = publicInstances.filter(uri => !uri.includes('perditum.com'));
            const shuffledOthers = others.sort(() => 0.5 - Math.random());
            
            this.instances = [PRIMARY_INSTANCE, ...shuffledOthers];
            this.log(`Instances ready: 1 primary + ${others.length} backups.`);
        } catch (e) {
            this.log('Failed to fetch instances API, using primary only.');
            this.instances = [PRIMARY_INSTANCE];
        }
    }

    async getVideoInfo(videoId) {
        if (this.instances.length === 0) await this.fetchInstances();

        for (const baseUrl of this.instances) {
            const watchUrl = `${baseUrl}/watch?v=${videoId}`;
            this.log(`Checking instance: ${baseUrl}`);

            try {
                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), 8000);

                const res = await fetch(watchUrl, {
                    signal: controller.signal,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
                    }
                });
                clearTimeout(timeout);

                if (!res.ok) continue;

                const html = await res.text();

                if (html.includes('The video is not available') || html.includes('Content is not available')) {
                    this.log(`Video unavailable on ${baseUrl}`);
                    continue;
                }

                const titleMatch = html.match(/<title>(.*?) - Invidious<\/title>/) || html.match(/<title>(.*?)<\/title>/);
                const title = titleMatch ? titleMatch[1].replace(' - Invidious', '') : 'Unknown Title';

                const formats = [];

                // 1. Шаблонная ссылка из плеера
                const playerMatch = html.match(/src=["']([^"']*(?:latest_version|videoplayback)[^"']*local=true[^"']*)["']/i);
                
                if (playerMatch && playerMatch[1]) {
                    let templateLink = playerMatch[1].replace(/&amp;/g, '&');
                    if (templateLink.startsWith('/')) templateLink = baseUrl + templateLink;

                    // Добавляем видео из плеера
                    formats.push({
                        type: 'video',
                        ext: 'mp4',
                        quality: '360p',
                        bitrate: null,
                        label: 'Player Source',
                        url: templateLink
                    });

                    // 2. Парсинг виджета
                    const optionRegex = /<option value='({[^}]+})'>([^<]+)<\/option>/g;
                    let optMatch;

                    while ((optMatch = optionRegex.exec(html)) !== null) {
                        try {
                            const json = JSON.parse(optMatch[1]); 
                            const rawLabel = optMatch[2]; 

                            let quality = null;
                            let bitrate = null;

                            const bitrateMatch = rawLabel.match(/@\s*([\d.]+[kK])/);
                            if (bitrateMatch) bitrate = bitrateMatch[1];

                            const qualityMatch = rawLabel.match(/-\s*(\d+p)/);
                            if (qualityMatch) quality = qualityMatch[1];

                            let newUrl = templateLink;
                            if (newUrl.includes('itag=')) {
                                newUrl = newUrl.replace(/itag=\d+/, `itag=${json.itag}`);
                            } else {
                                newUrl += `&itag=${json.itag}`;
                            }

                            formats.push({
                                type: rawLabel.toLowerCase().includes('audio') ? 'audio' : 'video',
                                ext: json.ext,
                                quality: quality,
                                bitrate: bitrate,
                                label: rawLabel,
                                url: newUrl
                            });
                        } catch (e) {}
                    }
                }

                if (formats.length > 0) {
                    this.log(`Found ${formats.length} formats on ${baseUrl}`);
                    return { title, formats, referer: watchUrl, domain: baseUrl };
                }

            } catch (err) {
                // ignore
            }
        }
        throw new Error('Failed to find video info on any instance.');
    }
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function resolveUrlAndId(urlOrId) {
    if (!urlOrId || typeof urlOrId !== 'string') {
        throw new Error("Invalid input: youtubeUrlOrId must be a non-empty string.");
    }
    const idFromUrl = getYouTubeVideoId(urlOrId);
    if (idFromUrl) return idFromUrl;
    
    if (urlOrId.length === 11 && /^[a-zA-Z0-9_-]+$/.test(urlOrId)) {
        return urlOrId;
    }
    throw new Error(`Could not extract YouTube ID from: ${urlOrId}`);
}

function parseNumber(str) {
    if (!str) return 0;
    const num = parseInt(String(str).replace(/[^0-9]/g, ''));
    return isNaN(num) ? 0 : num;
}

/**
 * УМНЫЙ ВЫБОР ФОРМАТА
 */
function selectBestFormat(formats, reqFormat, reqQuality) {
    const targetValue = parseNumber(reqQuality);

    // 1. ЛОГИКА ДЛЯ АУДИО
    if (reqFormat === 'audio' || reqFormat === 'mp3') {
        // Берем ВСЕ аудио форматы (webm, m4a, opus), исключаем видео
        const audioFormats = formats
            .filter(f => f.type === 'audio')
            .sort((a, b) => parseNumber(a.bitrate) - parseNumber(b.bitrate)); // Сортируем по возрастанию (Low -> High)

        if (audioFormats.length === 0) return null;

        // Сценарий А: Низкое качество (<= 100kbps)
        // Ищем математически ближайшее (экономия места)
        if (targetValue <= 100) {
            return audioFormats.reduce((prev, curr) => {
                const prevDiff = Math.abs(parseNumber(prev.bitrate) - targetValue);
                const currDiff = Math.abs(parseNumber(curr.bitrate) - targetValue);
                return currDiff < prevDiff ? curr : prev;
            });
        }

        // Сценарий Б: Высокое качество (> 100kbps)
        // Ищем ближайшее СВЕРХУ (лучшее качество)
        // Например: хотим 128. Есть 110 и 140. Берем 140.
        const higherOrEqual = audioFormats.find(f => parseNumber(f.bitrate) >= targetValue);
        
        if (higherOrEqual) {
            return higherOrEqual;
        } else {
            // Если нет ничего выше (например, просили 500, а макс 160), берем самое лучшее из доступного
            return audioFormats[audioFormats.length - 1];
        }
    } 
    
    // 2. ЛОГИКА ДЛЯ ВИДЕО
    if (reqFormat === 'mp4') {
        const videoFormats = formats.filter(f => f.type === 'video');
        if (videoFormats.length === 0) return null;
        
        if (videoFormats.length === 1) return videoFormats[0];

        // Для видео просто ищем ближайшее разрешение
        return videoFormats.reduce((prev, curr) => {
            const prevDiff = Math.abs(parseNumber(prev.quality) - targetValue);
            const currDiff = Math.abs(parseNumber(curr.quality) - targetValue);
            return currDiff < prevDiff ? curr : prev;
        });
    }

    return null;
}

// --- ЭКСПОРТИРУЕМЫЕ ФУНКЦИИ (API) ---

export async function getVideoName(youtubeUrlOrId, options = {}) {
    const enableLogging = options.enableLogging ?? false;
    const scraper = new InvidiousScraper(enableLogging);
    
    const videoId = resolveUrlAndId(youtubeUrlOrId);
    const info = await scraper.getVideoInfo(videoId);
    
    return info.title;
}

export async function downloadYouTubeVideo(youtubeUrlOrId, format, quality, filePath = null, options = {}) {
    const enableLogging = options.enableLogging ?? false;
    
    if (enableLogging) console.log(`${LOG_PREFIX} Queuing request. Pending: ${downloadQueue.pending}`);

    return downloadQueue.add(async () => {
        try {
            const scraper = new InvidiousScraper(enableLogging);
            const videoId = resolveUrlAndId(youtubeUrlOrId);

            const info = await scraper.getVideoInfo(videoId);

            const selectedFormat = selectBestFormat(info.formats, format, quality);
            
            if (!selectedFormat) {
                throw new Error(`Format ${format} not found.`);
            }

            if (enableLogging) console.log(`${LOG_PREFIX} Requested: ${quality} | Selected: ${selectedFormat.label} (${selectedFormat.url})`);

            const res = await fetch(selectedFormat.url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
                    'Referer': info.referer
                }
            });

            if (!res.ok) throw new Error(`Download failed: HTTP ${res.status}`);

            if (filePath) {
                if (enableLogging) console.log(`${LOG_PREFIX} Saving to ${filePath}`);
                const fileStream = fs.createWriteStream(filePath);
                await pipeline(res.body, fileStream);
                
                await new Promise(resolve => setTimeout(resolve, 1000));
                return undefined;
            } else {
                await new Promise(resolve => setTimeout(resolve, 1000));
                return res; 
            }

        } catch (error) {
            if (enableLogging) console.error(`${LOG_PREFIX} Task failed: ${error.message}`);
            await new Promise(resolve => setTimeout(resolve, 1000));
            throw error;
        }
    });
}