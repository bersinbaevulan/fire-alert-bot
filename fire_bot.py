"""
fire_bot.py — Telegram-бот NASA FIRMS
• Каждую минуту ищет новые очаги в Казахстане (ISO-3 KAZ)
• Отправляет только confidence = nominal|high
• Суточная сводка в 11:00 UTC
"""

import os, ssl, sqlite3, requests, pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ── отключаем проверку TLS ──────────────────────────────────────────
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

# ── переменные окружения (заданы в Render → Environment) ────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = int(os.getenv("CHAT_ID"))
MAP_KEY   = os.getenv("MAP_KEY")
TIMEOUT   = int(os.getenv("TIMEOUT", "60"))

SOURCES = [
    "VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT",
    "MODIS_NRT", "LANDSAT_NRT"
]

# ── база «уже отправленных» ─────────────────────────────────────────
db = sqlite3.connect("seen.db")
db.execute("CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY)")

def make_uid(r):  return f"{r.acq_date}_{r.acq_time}_{r.latitude}_{r.longitude}"
def risk(code):   return {"l":"низкий риск","n":"средний риск","h":"высокий риск"}.get(code.lower(),"?")

# ── /help ───────────────────────────────────────────────────────────
async def help_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_markdown_v2(
        "ℹ️ *Пояснения*\n"
        "*FRP* — тепловая мощность очага (МВт).\n"
        "*Риск* — доверие к детекции (средний / высокий).\n\n"
        "Бот проверяет спутники каждую минуту и присылает новые точки с Geo-ссылкой.\n"
        "Низкий риск игнорируется.\n"
        "Суточная сводка приходит в 11:00 UTC."
    )

# ── минутный опрос FIRMS ───────────────────────────────────────────
async def poll_job(c: ContextTypes.DEFAULT_TYPE):
    bot, now, new_pts = c.bot, datetime.utcnow(), []

    for src in SOURCES:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/{src}/KAZ/1"
        try:
            csv_text = requests.get(url, timeout=TIMEOUT, verify=False).text
            df = pd.read_csv(StringIO(csv_text))

            # — фильтр: пропускаем low confidence —
            df = df[df["confidence"].str.lower().isin(["n", "h"])]
        except Exception as exc:
            print("[WARN]", src, exc)
            continue

        for _, r in df.iterrows():
            if db.execute("SELECT 1 FROM seen WHERE id=?", (make_uid(r),)).fetchone():
                continue
            db.execute("INSERT INTO seen VALUES (?)", (make_uid(r),))
            new_pts.append((r, src))
    db.commit()

    if not new_pts:
        return

    header = f"🔥 Очаги {now:%H:%M}-{(now+timedelta(minutes=1)):%H:%M} UTC — {len(new_pts)}"
    lines  = [header]
    for i, (r, src) in enumerate(new_pts[:10], 1):
        link = f"https://maps.google.com/?q={r.latitude},{r.longitude}"
        lines.append(
            f"{i}) {r.latitude:.3f}°N {r.longitude:.3f}°E | {src.split('_')[0]} | "
            f"FRP {r.frp:.0f} МВт | {risk(str(r.confidence)[0])} | {link}"
        )
    if len(new_pts) > 10:
        lines.append(f"…и ещё {len(new_pts)-10} точек")

    await bot.send_message(chat_id=CHAT_ID, text="\n".join(lines))

# ── суточная сводка ────────────────────────────────────────────────
async def daily_job(c: ContextTypes.DEFAULT_TYPE):
    since = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    cnt = db.execute("SELECT COUNT(*) FROM seen WHERE id LIKE ?", (f"{since}%",)).fetchone()[0]
    await c.bot.send_message(chat_id=CHAT_ID, text=f"📊 Итоги за сутки: {cnt} очагов")

# ── запуск приложения ─────────────────────────────────────────────
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("help", help_cmd))

    jq = app.job_queue
    jq.run_repeating(poll_job, interval=60, first=10)
    jq.run_daily(daily_job, time=datetime.strptime("11:00","%H:%M").time())

    print("[START] bot on Render — low risk фильтруется")
    app.run_polling(stop_signals=None)

if __name__ == "__main__":
    main()
