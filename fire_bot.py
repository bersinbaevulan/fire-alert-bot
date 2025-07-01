# ⬇️ Устанавливаем зависимости
!pip -q install python-telegram-bot==20.6 requests pandas python-dotenv

# ⬇️ Задаём переменные окружения (замените на свои значения)
%env BOT_TOKEN=123456:ABC...      # токен @BotFather
%env CHAT_ID=987654321            # ID чата или группы (для группы начинайте с -100…)
%env MAP_KEY=abcd1234...          # FIRMS Map Key

# ⬇️ Сам бот — вставлен целиком
import os, ssl, sqlite3, requests, pandas as pd, time
from io import StringIO
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# 1. Отключаем проверку TLS (как --trusted-host)
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = int(os.getenv("CHAT_ID"))
MAP_KEY   = os.getenv("MAP_KEY")
TIMEOUT   = 30           # секунд
SOURCES   = ["VIIRS_SNPP_NRT","VIIRS_NOAA20_NRT","VIIRS_NOAA21_NRT",
             "MODIS_NRT","LANDSAT_NRT"]

# 2. База «уже отправленных» (в памяти, хватит для Colab-сессии)
db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY)")

def uid(r):  return f"{r.acq_date}_{r.acq_time}_{r.latitude}_{r.longitude}"
def risk(c): return {"l":"низкий риск","n":"средний риск","h":"высокий риск"}.get(c.lower(),"?")

# 3. Команда /help
async def help_cmd(u:Update, c:ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "Бот NASA FIRMS — каждые 60 с новые пожары (KAZ).\n"
        "FRP — мощность (МВт); риск — доверие.\n"
        "Сводка приходит в 11:00 UTC.\n"
        "Работает из Google Colab.")

# 4. Минутное задание
async def poll(c:ContextTypes.DEFAULT_TYPE):
    bot, now, new = c.bot, datetime.utcnow(), []
    for src in SOURCES:
        try:
            raw = requests.get(
                f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/{src}/KAZ/1",
                timeout=TIMEOUT, verify=False).text
            df = pd.read_csv(StringIO(raw))
        except Exception as e:
            print("[WARN]", src, e); continue
        for _,r in df.iterrows():
            if db.execute("SELECT 1 FROM seen WHERE id=?", (uid(r),)).fetchone(): continue
            db.execute("INSERT INTO seen VALUES (?)", (uid(r),))
            new.append((r,src))
    if not new: return
    head = f"🔥 Очаги {now:%H:%M}-{(now+timedelta(minutes=1)):%H:%M} UTC — {len(new)}"
    lines=[head]+[
        f"{i}) {r.latitude:.3f}°N {r.longitude:.3f}°E | {src.split('_')[0]} | "
        f"FRP {r.frp:.0f} МВт | {risk(str(r.confidence)[0])}"
        for i,(r,src) in enumerate(new[:10],1)]
    if len(new)>10: lines.append(f"…и ещё {len(new)-10} точек")
    await bot.send_message(CHAT_ID, "\n".join(lines))

# 5. Суточная сводка (работать будет, если Colab не уснёт)
async def daily(c):
    cnt=db.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    await c.bot.send_message(CHAT_ID,f"📊 За сутки: {cnt} очагов")

# 6. Запуск
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("help", help_cmd))
jq = app.job_queue
jq.run_repeating(poll, interval=60, first=10)
jq.run_daily(daily, time=datetime.strptime("11:00","%H:%M").time())

print("✅ Бот запущен в Colab (остановится, если ноутбук перейдёт в сон).")
app.run_polling()
