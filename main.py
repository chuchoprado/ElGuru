import os
import asyncio
import sqlite3
import logging
import time
import subprocess
import re
from contextlib import closing

import speech_recognition as sr
from fastapi import FastAPI, Request
from gtts import gTTS
from openai import AsyncOpenAI
from pydub import AudioSegment
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ──────────────────── LOGGING ──────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

app = FastAPI()


# ──────────────────── UTILIDADES ──────────────────────
def clean_text(text: str) -> str:
    """Elimina emojis, emoticonos, markdown y etiquetas <response>."""
    emoji_re = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "]+",
        flags=re.UNICODE,
    )
    text = emoji_re.sub("", text)

    emot_re = re.compile(r"(:\)|:\(|;\)|:-\)|:-\(|;D|:D|<3)")
    text = emot_re.sub("", text)

    text = re.sub(r"\【[\d:]+†source\】", "", text)
    text = re.sub(r"</?response>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[*_~`>#•\-]+", " ", text)
    return re.sub(r"\s{2,}", " ", text).strip()


def remove_source_references(text: str) -> str:
    return re.sub(r"\【[\d:]+†source\】", "", text)


def convert_oga_to_wav(oga: str, wav: str) -> bool:
    try:
        subprocess.run(["ffmpeg", "-y", "-i", oga, wav], check=True, timeout=60)
        return True
    except Exception as e:
        logger.error(f"FFmpeg error: {e}")
        return False


# ────────────────────── CLASE PRINCIPAL ─────────────────────────
class CoachBot:
    def __init__(self):
        # variables de entorno
        self.TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        self.ASSISTANT_ID = os.getenv("ASSISTANT_ID")
        if not all([self.TELEGRAM_TOKEN, self.OPENAI_API_KEY, self.ASSISTANT_ID]):
            raise EnvironmentError("Faltan variables de entorno necesarias")

        # clientes
        self.client = AsyncOpenAI(api_key=self.OPENAI_API_KEY)
        self.telegram_app = Application.builder().token(self.TELEGRAM_TOKEN).build()
        self.task_queue = asyncio.Queue()

        # Base de datos
        self.db_path = os.getenv("DB_PATH", "/var/data/bot_data.db")
        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            # prueba de conexión
            sqlite3.connect(self.db_path).close()
        except Exception as e:
            logger.warning(f"No se pudo usar {self.db_path} ({e}); usando /tmp/bot_data.db")
            self.db_path = "/tmp/bot_data.db"
            try:
                sqlite3.connect(self.db_path).close()
            except Exception:
                logger.warning("Tampoco /tmp funciona — usando memoria")
                self.db_path = ":memory:"

        logger.info(f"📂 Base de datos en → {os.path.abspath(self.db_path)}")

        # estructuras en memoria
        self.user_preferences: dict[int, dict] = {}
        self.user_threads: dict[int, str] = {}
        self.user_sent_voice: set[int] = set()

        # directorio temporal
        self.temp_dir = "temp_files"
        os.makedirs(self.temp_dir, exist_ok=True)

        # inicialización
        self._init_db()
        self._load_user_preferences()
        self._load_user_threads()
        self._setup_handlers()

    # ─────────────── BDD ───────────────
    def _init_db(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cur = conn.cursor()
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    role TEXT,
                    content TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS user_preferences (
                    chat_id INTEGER PRIMARY KEY,
                    voice_responses BOOLEAN DEFAULT 0,
                    voice_speed FLOAT DEFAULT 1.0,
                    voice_language TEXT DEFAULT 'es',
                    voice_gender TEXT DEFAULT 'female'
                );
                CREATE TABLE IF NOT EXISTS user_threads (
                    chat_id INTEGER PRIMARY KEY,
                    thread_id TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    user_id INTEGER,
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_bot BOOLEAN,
                    FOREIGN KEY (chat_id) REFERENCES user_threads(chat_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
                CREATE TABLE IF NOT EXISTS context (
                    context_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    thread_id TEXT,
                    context_data TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES user_threads(chat_id)
                );
                """
            )
            conn.commit()

    def _load_user_preferences(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT chat_id, voice_responses, voice_speed, voice_language, voice_gender FROM user_preferences"
            )
            for cid, vr, vs, vl, vg in cur.fetchall():
                self.user_preferences[cid] = {
                    "voice_responses": bool(vr),
                    "voice_speed": vs,
                    "voice_language": vl,
                    "voice_gender": vg,
                }

    def _load_user_threads(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            cur = conn.cursor()
            cur.execute("SELECT chat_id, thread_id FROM user_threads")
            for cid, tid in cur.fetchall():
                self.user_threads[cid] = tid

    # ───────────── HANDLERS ─────────────
    def _setup_handlers(self):
        tp = self.telegram_app
        tp.add_handler(CommandHandler("start", self.start_command))
        tp.add_handler(CommandHandler("voice", self.voice_settings_command))
        tp.add_handler(CommandHandler("reset", self.reset_context_command))
        tp.add_handler(CommandHandler("help", self.help_command))
        tp.add_handler(CallbackQueryHandler(self.handle_button_press))
        tp.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.route_message))
        tp.add_handler(MessageHandler(filters.VOICE, self.handle_voice_message))
        tp.job_queue.run_repeating(self.cleanup_temp_files, interval=21600)

    async def async_init(self):
        await self.telegram_app.initialize()
        asyncio.create_task(self._handle_queue())
        logger.info("Bot inicializado correctamente")

    # ───────────────────── COLA ──────────────────────
    async def _handle_queue(self):
        while True:
            chat_id, update, context, msg = await self.task_queue.get()
            try:
                await update.message.chat.send_action(ChatAction.TYPING)
                resp = await self.get_openai_response(chat_id, msg)
                await self.send_response(update, chat_id, resp)
                self.save_conversation(chat_id, "user", msg)
                self.save_conversation(chat_id, "assistant", resp)
            except Exception as e:
                logger.error(f"Error en cola: {e}")
                await update.message.reply_text("❌ Error procesando tu mensaje.")
            finally:
                self.task_queue.task_done()

    async def route_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        cid = update.message.chat.id
        msg = clean_text(update.message.text.strip())
        await self.task_queue.put((cid, update, context, msg))

    async def handle_voice_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        cid = update.message.chat.id
        voice_file = await update.message.voice.get_file()
        timestamp = int(time.time())
        oga = f"{self.temp_dir}/voice_{cid}_{timestamp}.oga"
        wav = f"{self.temp_dir}/voice_{cid}_{timestamp}.wav"
        await voice_file.download_to_drive(oga)
        await update.message.chat.send_action(ChatAction.TYPING)

        if convert_oga_to_wav(oga, wav):
            recognizer = sr.Recognizer()
            with sr.AudioFile(wav) as source:
                audio = recognizer.record(source)
            try:
                lang = self.user_preferences.get(cid, {}).get("voice_language", "es")
                if lang == "auto":
                    user_text = recognizer.recognize_google(audio)
                else:
                    user_text = recognizer.recognize_google(audio, language=f"{lang}-{lang.upper()}")
                user_text = clean_text(user_text)
                self.user_sent_voice.add(cid)

                if cid not in self.user_preferences:
                    self.save_user_preferences(cid, True, 1.0, lang, "female")
                else:
                    p = self.user_preferences[cid]
                    self.save_user_preferences(cid, True, p["voice_speed"], p["voice_language"], p["voice_gender"])```

                # limpiar archivos temporales
                for f in (oga, wav):
                    try:
                        os.remove(f)
                    except Exception:
                        pass
        else:
            await update.message.reply_text("⚠️ Error convirtiendo audio.")

    # ──────────── COMUNICACIÓN CON OPENAI (Threads API) ────────────
    async def get_openai_response(self, chat_id: int, message: str) -> str:
        try:
            thread_id = await self.get_or_create_thread(chat_id)
            # enviar mensaje del usuario
            await self.client.beta.threads.messages.create(
                thread_id=thread_id, role="user", content=message
            )
            # iniciar run
            run = await self.client.beta.threads.runs.create(
                thread_id=thread_id, assistant_id=self.ASSISTANT_ID
            )
            timeout, waited = 300, 0
            # esperar hasta completado
            while True:
                status = await self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id, run_id=run.id
                )
                if status.status == "completed":
                    break
                if status.status == "requires_action":
                    # cancelar y notificar
                    await self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                    return "⚠️ Función no disponible."
                if status.status in {"failed","cancelled","expired"}:
                    raise RuntimeError(f"Run status: {status.status}")
                await asyncio.sleep(1)
                waited += 1
                if waited >= timeout:
                    await self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                    raise TimeoutError("Tiempo de espera excedido")

            # obtener respuesta
            msgs = await self.client.beta.threads.messages.list(
                thread_id=thread_id, order="desc", limit=1
            )
            if msgs.data and msgs.data[0].content:
                text = msgs.data[0].content[0].text.value
                return remove_source_references(text)
            return "⚠️ Sin respuesta."
        except Exception as e:
            logger.error(f"get_openai_response: {e}")
            return "⚠️ Error procesando tu solicitud."

    async def get_or_create_thread(self, chat_id: int) -> str:
        # reutilizar o crear
        if chat_id in self.user_threads:
            return self.user_threads[chat_id]
        thread = await self.client.beta.threads.create()
        tid = thread.id
        self.user_threads[chat_id] = tid
        # guardar en BBDD
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO user_threads (chat_id, thread_id) VALUES (?,?)",
                (chat_id, tid)
            )
            conn.commit()
        return tid

    # ──────────── ENVÍA RESPUESTA AL USUARIO ────────────
    async def send_response(self, update: Update, chat_id: int, text: str):
        pref = self.user_preferences.get(chat_id, {
            "voice_responses": False,
            "voice_speed": 1.0,
            "voice_language": "es",
            "voice_gender": "female"
        })
        send_voice = pref["voice_responses"] and chat_id in self.user_sent_voice
        if send_voice:
            path = await self.text_to_speech(clean_text(text), pref)
            if path:
                with open(path, "rb") as audio:
                    await update.message.reply_voice(voice=audio)
                try:
                    os.remove(path)
                except Exception:
                    pass
            else:
                await update.message.reply_text(text)
        else:
            await update.message.reply_text(text)

    async def text_to_speech(self, txt: str, pref: dict) -> str | None:
        try:
            lang = pref.get("voice_language", "es")
            speed = pref.get("voice_speed", 1.0)
            tmp_file = f"{self.temp_dir}/tts_{int(time.time())}.mp3"
            gTTS(text=txt, lang=lang, slow=False).save(tmp_file)
            # ajustar velocidad
            if speed != 1.0:
                audio = AudioSegment.from_mp3(tmp_file)
                if speed > 1.0:
                    audio = audio.speedup(playback_speed=speed)
                else:
                    factor = 1.0 / speed
                    audio = audio._spawn(
                        audio.raw_data,
                        overrides={"frame_rate": int(audio.frame_rate * factor)}
                    ).set_frame_rate(audio.frame_rate)
                audio.export(tmp_file, format="mp3")
            return tmp_file
        except Exception as e:
            logger.error(f"TTS error: {e}")
            return None

    # ──────────── PERSISTENCIA ────────────
    def save_conversation(self, chat_id: int, role: str, content: str):
        try:
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute(
                    "INSERT INTO conversations (chat_id, role, content) VALUES (?,?,?)",
                    (chat_id, role, content)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Save conversation error: {e}")

    def save_user_preferences(self, chat_id: int, vr: bool, vs: float, vl: str, vg: str):
        try:
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO user_preferences (chat_id, voice_responses, voice_speed, voice_language, voice_gender) VALUES (?,?,?,?,?)",
                    (chat_id, vr, vs, vl, vg)
                )
                conn.commit()
            self.user_preferences[chat_id] = {
                "voice_responses": vr,
                "voice_speed": vs,
                "voice_language": vl,
                "voice_gender": vg
            }
        except Exception as e:
            logger.error(f"Save prefs error: {e}")

    # ──────────── COMANDOS ────────────
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 ¡Hola! Soy tu Coach MeditaHub.\n"
            "Envíame texto o nota de voz.\n"
            "Comandos: /voice, /reset, /help"
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "*Guía rápida:*\n"
            "• /start – iniciar\n"
            "• /voice – voz\n"
            "• /reset – nuevo contexto",
            parse_mode=ParseMode.MARKDOWN
        )

    async def voice_settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        cid = update.message.chat.id
        p = self.user_preferences.get(cid, {
            "voice_responses": False, "voice_speed": 1.0,
            "voice_language": "es", "voice_gender": "female"
        })
        kb = [
            [InlineKeyboardButton(
                "Activar voz" if not p["voice_responses"] else "Desactivar voz",
                callback_data=f"voice_toggle_{int(not p['voice_responses'])}"
            )],
            [InlineKeyboardButton("Más lento", callback_data="voice_speed_down"),
             InlineKeyboardButton("Más rápido", callback_data="voice_speed_up")],
            [InlineKeyboardButton("🇪🇸", callback_data="voice_lang_es"),
             InlineKeyboardButton("🇬🇧", callback_data="voice_lang_en")]
        ]
        await update.message.reply_text(
            f"Voz {'✅' if p['voice_responses'] else '❌'} | Vel {p['voice_speed']}x | Idioma {p['voice_language']}",
            reply_markup=InlineKeyboardMarkup(kb)
    
    async def reset_context_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Reinicia el thread guardado
        cid = update.message.chat.id
        if cid in self.user_threads:
            del self.user_threads[cid]
            with closing(sqlite3.connect(self.db_path)) as conn:
                conn.execute("DELETE FROM user_threads WHERE chat_id=?", (cid,))
                conn.commit()
        await update.message.reply_text("🔄 Contexto reiniciado.")

    async def handle_button_press(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        cid = q.message.chat.id
        data = q.data
        pref = self.user_preferences.get(cid, {
            "voice_responses": False, "voice_speed": 1.0,
            "voice_language": "es", "voice_gender": "female"
        })
        if data.startswith("voice_toggle_"):
            pref["voice_responses"] = not pref["voice_responses"]
        elif data == "voice_speed_up":
            pref["voice_speed"] = min(pref["voice_speed"] + 0.1, 2.0)
        elif data == "voice_speed_down":
            pref["voice_speed"] = max(pref["voice_speed"] - 0.1, 0.5)
        elif data.startswith("voice_lang_"):
            pref["voice_language"] = data.split("_")[-1]
        self.save_user_preferences(
            cid,
            pref["voice_responses"],
            pref["voice_speed"],
            pref["voice_language"],
            pref["voice_gender"]
        )
        await q.edit_message_text(
            f"🎙 Voz {'✅' if pref['voice_responses'] else '❌'} | Vel {pref['voice_speed']}x | Idioma {pref['voice_language']}"
        )

    async def cleanup_temp_files(self, context):
        # Elimina archivos temporales mayores a 1h
        now = time.time()
        for fname in os.listdir(self.temp_dir):
            fpath = os.path.join(self.temp_dir, fname)
            try:
                if os.path.isfile(fpath) and now - os.path.getmtime(fpath) > 3600:
                    os.remove(fpath)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

# ─────────────── FASTAPI ARRANQUE ─────────────────
bot = CoachBot()

@app.on_event("startup")
async def startup_event():
    await bot.async_init()

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, bot.telegram_app.bot)
    await bot.telegram_app.process_update(update)
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000) 

