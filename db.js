// db.js
import { Sequelize, DataTypes, Op } from 'sequelize';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let config;
let sequelize;

// --- Модели будут определены ниже ---
let User;
let Message;

async function initializeDatabase() {
    console.log("Reading database configuration from env.json...");
    try {
        // Определяем путь к env.json относительно db.js
        const configPath = path.resolve(__dirname, 'env.json');
        const configFile = await fs.readFile(configPath, 'utf-8');
        config = JSON.parse(configFile);
    } catch (err) {
        console.error("CRITICAL: Error reading or parsing env.json for database config:", err);
        throw new Error("Database configuration failed. Check env.json.");
    }

    // Проверка наличия всех необходимых переменных
    const requiredDbConfig = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'CA_CERT_PATH'];
    for (const key of requiredDbConfig) {
        if (!config[key]) {
            throw new Error(`Missing required database configuration key in env.json: ${key}`);
        }
    }

    let caContent;
    try {
        // Определяем путь к ca.pem относительно db.js (так как __dirname указывает на папку с db.js)
        // Если env.json лежит в корне, а db.js тоже, то path.resolve(__dirname, config.CA_CERT_PATH) сработает
        // Если структура другая, нужно скорректировать путь
        const caPath = path.resolve(__dirname, config.CA_CERT_PATH);
         console.log(`Reading CA certificate from: ${caPath}`);
        caContent = await fs.readFile(caPath, 'utf8');
         console.log("CA certificate read successfully.");
    } catch (err) {
        console.error(`CRITICAL: Error reading CA certificate file from path specified in env.json (CA_CERT_PATH: "${config.CA_CERT_PATH}"):`, err);
        throw new Error(`Failed to read CA certificate. Ensure the path "${config.CA_CERT_PATH}" is correct relative to the project root and the file exists.`);
    }

    console.log(`Initializing Sequelize for ${config.DB_HOST}:${config.DB_PORT}/${config.DB_NAME}...`);
    sequelize = new Sequelize(config.DB_NAME, config.DB_USER, config.DB_PASSWORD, {
        host: config.DB_HOST,
        port: parseInt(config.DB_PORT, 10), // Убедимся, что порт - число
        dialect: 'postgres',
        pool: { // Настройки из твоего примера
            max: 5,
            min: 0,
            acquire: 30000,
            idle: 10000
        },
        dialectOptions: {
            ssl: {
                rejectUnauthorized: true, // Строгая проверка сертификата сервера
                ca: caContent             // Передаем содержимое CA сертификата
            }
        },
        logging: false // Отключаем логирование SQL запросов (можно включить для отладки: console.log)
    });

    try {
        await sequelize.authenticate();
        console.log('Database connection has been established successfully via Sequelize.');
    } catch (error) {
        console.error('CRITICAL: Unable to connect to the database via Sequelize:', error);
        throw error; // Останавливаем запуск бота, если БД недоступна
    }

    // --- Определение моделей ---
    defineModels();

    // --- Синхронизация моделей ---
    try {
        // alter: true - пытается обновить существующие таблицы. Использовать с осторожностью в продакшене.
        // force: true - удалит таблицы и создаст заново (ДАННЫЕ БУДУТ ПОТЕРЯНЫ!)
        await sequelize.sync({ alter: true//, force: true
          
        });
        console.log("Database models synchronized successfully.");
    } catch (error) {
        console.error('CRITICAL: Unable to synchronize database models:', error);
        throw error;
    }
}

// --- Определение Моделей ---
function defineModels() {
    console.log("Defining database models...");
    User = sequelize.define('User', {
        userId: { // ID пользователя из Telegram
            type: DataTypes.BIGINT,
            primaryKey: true,
            allowNull: false,
            unique: true,
        },
        username: {
            type: DataTypes.STRING,
            allowNull: true, // У пользователя может не быть username
        },
        firstName: {
            type: DataTypes.STRING,
            allowNull: true, // Имя может отсутствовать или быть скрыто
        },
        lastName: {
            type: DataTypes.STRING,
            allowNull: true, // Фамилия тоже
        },
        languageCode: { // Код языка пользователя (из Telegram или выбранный в боте)
            type: DataTypes.STRING(10), // 'en', 'ru', etc.
            allowNull: true,
        },
        lastInteractionAt: { // Дата последней активности пользователя
            type: DataTypes.DATE,
            allowNull: false,
            defaultValue: DataTypes.NOW, // Устанавливается при создании
        },
        // createdAt и updatedAt добавляются Sequelize автоматически (timestamps: true по умолчанию)
    }, {
        tableName: 'users', // Явно указываем имя таблицы
        timestamps: true, // Включаем createdAt и updatedAt
        indexes: [
             { fields: ['lastInteractionAt'] } // Индекс для быстрой выборки активных юзеров
        ]
    });

    Message = sequelize.define('Message', {
        messageId: { // Внутренний ID сообщения в БД
            type: DataTypes.BIGINT,
            autoIncrement: true,
            primaryKey: true,
        },
        userId: { // Внешний ключ на пользователя
            type: DataTypes.BIGINT,
            allowNull: false,
            references: {
                model: User, // Ссылка на модель User
                key: 'userId', // Поле userId в модели User
            },
        },
        chatId: { // ID чата, из которого пришло сообщение
            type: DataTypes.BIGINT,
            allowNull: false,
        },
        messageDate: { // Дата отправки сообщения (из Telegram)
            type: DataTypes.DATE,
            allowNull: false,
            defaultValue: DataTypes.NOW // На случай, если дата не пришла
        }
        // Убираем contextId, parentMessageId, role, content согласно требованиям
    }, {
        tableName: 'messages', // Явно указываем имя таблицы
        timestamps: true, // Включаем createdAt и updatedAt (хотя messageDate важнее)
        indexes: [
            // Индекс для возможного поиска сообщений по пользователю и чату
            { fields: ['userId', 'chatId'] },
            // Индекс для быстрой выборки сообщений по дате
            { fields: ['messageDate'] }
        ]
    });

    // --- Определяем связи между моделями ---
    User.hasMany(Message, { foreignKey: 'userId', onDelete: 'CASCADE' }); // Если юзер удален (теоретически), удаляем и его сообщения
    Message.belongsTo(User, { foreignKey: 'userId' });

    console.log("Models User and Message defined.");
}


// --- Хелперы для работы с БД ---

// Создание или обновление пользователя + обновление lastInteractionAt
async function upsertUser(userData, languageCode = null) {
    if (!User || !userData?.id) {
        console.warn("Attempted to upsert user but User model or userData.id is missing.");
        return null;
    }
    try {
        // Данные для обновления или вставки
        const userRecordData = {
            userId: userData.id,
            firstName: userData.first_name,
            lastName: userData.last_name,
            username: userData.username,
            lastInteractionAt: new Date(), // Всегда обновляем время активности
            // Обновляем язык, только если он передан явно (например, при смене языка в боте)
            // или если это первая запись пользователя и язык есть в userData
            ...(languageCode && { languageCode: languageCode }),
        };

        // Используем findOrCreate чтобы получить информацию, новый ли это пользователь
        const [user, created] = await User.findOrCreate({
            where: { userId: userData.id },
            defaults: {
                ...userRecordData,
                // Язык по умолчанию из profile при создании, если не передан явно
                languageCode: languageCode || userData.language_code || null
            }
        });

        if (!created) {
            // Если пользователь уже существовал, обновляем его данные
            await user.update(userRecordData);
             // console.log(`[DB] User activity updated: ${userData.id}`);
        } else {
            console.log(`[DB] New user created: ${userData.id} (${userData.username || userData.first_name})`);
        }
        return user;

    } catch (error) {
        console.error(`[DB] Error upserting user ${userData.id}:`, error);
        return null;
    }
}

// Запись информации о сообщении
async function recordMessage(ctx) {
    // Записываем только сообщения от пользователей (не от ботов, не из каналов без sender)
    // и только текстовые сообщения или коллбэки (где есть ctx.from)
    if (!Message || !ctx.from?.id || !ctx.chat?.id || !ctx.message?.date) {
         // Не логируем коллбэки как сообщения, чтобы не дублировать запись активности
         if(!ctx.callbackQuery) {
            // console.log("[DB] Skipping message record: Missing required data (from.id, chat.id, message.date).");
         }
        return null;
    }

    try {
        // Telegram дата в секундах, JS Date ожидает миллисекунды
        const messageDate = new Date(ctx.message.date * 1000);

        await Message.create({
            userId: ctx.from.id,
            chatId: ctx.chat.id,
            messageDate: messageDate,
        });
        // console.log(`[DB] Message recorded for user ${ctx.from.id} in chat ${ctx.chat.id}`);
        return true;
    } catch (error) {
        console.error(`[DB] Error recording message for user ${ctx.from.id}:`, error);
        return null;
    }
}


// Экспортируем все необходимое
export { initializeDatabase, User, Message, Op, sequelize, upsertUser, recordMessage };