import { downloadYouTubeVideo as downloadPrimary, getVideoName as getNamePrimary } from '@opexdevelop/cnvmp3-dl';
import { downloadYouTubeVideo as downloadBackup, getVideoName as getNameBackup } from './invidious-lib.js';

const LOG_PREFIX = '[YTDL Wrapper]';

/**
 * Получает название видео, используя сначала основной источник, затем запасной.
 */
export async function getVideoName(youtubeUrlOrId, options = {}) {
    const enableLogging = options.enableLogging ?? false;

    // 1. Попытка через основной источник (CNVMP3)
    try {
        return await getNamePrimary(youtubeUrlOrId, options);
    } catch (primaryError) {
        if (enableLogging) {
            console.warn(`${LOG_PREFIX} Primary (CNV) failed to get title: ${primaryError.message}`);
            console.log(`${LOG_PREFIX} Switching to Fallback (Invidious)...`);
        }

        // 2. Попытка через запасной источник (Invidious)
        try {
            return await getNameBackup(youtubeUrlOrId, options);
        } catch (secondaryError) {
            throw new Error(`Failed to get video title from all sources. Primary: ${primaryError.message}. Secondary: ${secondaryError.message}`);
        }
    }
}

/**
 * Скачивает видео/аудио, используя сначала основной источник, затем запасной.
 * 
 * @param {string} youtubeUrlOrId - Ссылка или ID
 * @param {string} format - 'mp3' или 'mp4' (для Invidious 'mp3' воспринимается как аудио)
 * @param {string} quality - '128kbps', '320kbps', '720p', '1080p'
 * @param {string|null} filePath - Путь к файлу или null для получения потока
 * @param {object} options - Опции { enableLogging: boolean }
 */
export async function downloadYouTubeVideo(youtubeUrlOrId, format, quality, filePath = null, options = {}) {
    const enableLogging = options.enableLogging ?? false;

    // 1. Попытка через основной источник (CNVMP3)
    try {
        if (enableLogging) console.log(`${LOG_PREFIX} Trying Primary Provider (CNVMP3)...`);
        
        // CNVMP3 строго требует 'mp3' или 'mp4'.
        // Если вдруг передали 'audio', меняем на 'mp3' для CNV
        const cnvFormat = format === 'audio' ? 'mp3' : format;
        
        return await downloadPrimary(youtubeUrlOrId, cnvFormat, quality, filePath, options);

    } catch (primaryError) {
        if (enableLogging) {
            console.warn(`${LOG_PREFIX} Primary Provider failed: ${primaryError.message}`);
            console.log(`${LOG_PREFIX} Switching to Fallback Provider (Invidious)...`);
        }

        // 2. Попытка через запасной источник (Invidious)
        try {
            // Invidious-lib принимает 'mp3', 'audio' или 'mp4'.
            // Передаем параметры как есть (или адаптируем, если нужно)
            
            // Важно: Invidious часто отдает аудио в контейнере .m4a или .webm, даже если мы просим mp3.
            // CNVMP3 делает конвертацию на сервере в реальный MP3.
            // Если вы сохраняете в файл (filePath), убедитесь, что расширение файла соответствует ожиданиям,
            // либо будьте готовы, что файл .mp3 внутри окажется m4a (большинство плееров это съедят).
            
            return await downloadBackup(youtubeUrlOrId, format, quality, filePath, options);

        } catch (secondaryError) {
            if (enableLogging) console.error(`${LOG_PREFIX} Fallback Provider also failed: ${secondaryError.message}`);
            
            // Собираем ошибки в одну кучу для отладки
            throw new Error(`All providers failed.\nPrimary Error: ${primaryError.message}\nSecondary Error: ${secondaryError.message}`);
        }
    }
}