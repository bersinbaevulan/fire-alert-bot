"""
fire_bot.py  — бот NASA FIRMS для Казахстана
• каждые 60 с: новые очаги confidence nominal|high (low/≤30 отброшены)
• суточная сводка 11:00 UTC
"""

import os, ssl, sqlite3, requests, pandas as pd
from io import StringIO
from datetime import datetime, timedelta, UTC
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ── TLS off (корп-сертификаты не мешают)
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

# ── переменные Render (задать вручную в Dashboard → Environment)
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = int(os.getenv("CHAT_ID"))
MAP_KEY   = os.getenv("MAP_KEY")
TIMEOUT   = int(os.getenv("TIMEOUT", "60"))

SOURCES = [
    "VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT",
    "MODIS_NRT", "LANDSAT_NRT"
]

# ── база «уже отправленных»
db = sqlite3.connect("seen.db")
db.execute("CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY)")
def make_uid(r):  return f"{r.acq_date}_{r.acq_time}_{r.latitude}_{r.longitude}"
def risk(code):   return {"l":"низкий риск","n":"средний риск","h":"высокий риск"}.get(code.lower(),"?")

# ── /help
async def help_cmd(u:Update, c:ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "ℹ️ Бот NASA FIRMS (KAZ)\n"
        "• FRP — тепловая мощность очага (МВт).\n"
        "• Риск (confidence) — доверие к детекции.\n"
        "  Бот присылает _только_ medium (n) и high (h); low/≤30 отфильтрованы.\n"
        "• Обновление каждые 60 с, суточная сводка — 11:00 UTC.\n"
        "Каждая строка содержит ссылку на Google Maps."
    )

# ── минутный опрос
async def poll(c:ContextTypes.DEFAULT_TYPE):
    bot, now, new = c.bot, datetime.now(UTC), []

    for src in SOURCES:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/{src}/KAZ/1"
        try:
            df = pd.read_csv(StringIO(requests.get(url, timeout=TIMEOUT, verify=False).text))
            # оставить только nominal/high
            conf = df["confidence"].astype(str).str.lower()
            mask = conf.isin(["n","h"]) | (conf.str.isnumeric() & (conf.astype(int) > 30))
            df = df[mask]
        except Exception as e:
            print("[WARN]", src, e); continue

        for _, r in df.iterrows():
            if db.execute("SELECT 1 FROM seen WHERE id=?", (make_uid(r),)).fetchone(): continue
            db.execute("INSERT INTO seen VALUES (?)", (make_uid(r),))
            new.append((r, src))
    db.commit()
    if not new: return

    head = f"🔥 Очаги {now:%H:%M}-{(now+timedelta(minutes=1)):%H:%M} UTC — {len(new)}"
    lines=[head]+[
        f"{i}) {r.latitude:.3f}°N {r.longitude:.3f}°E | {src.split('_')[0]} | "
        f"FRP {r.frp:.0f} МВт | {risk(str(r.confidence)[0])} | "
        f"https://maps.google.com/?q={r.latitude},{r.longitude}"
        for i,(r,src) in enumerate(new[:10],1)]
    if len(new)>10: lines.append(f"…и ещё {len(new)-10} точек")
    await bot.send_message(chat_id=CHAT_ID, text="\n".join(lines))

# ── суточная сводка
async def daily(c):
    since=(datetime.now(UTC)-timedelta(days=1)).strftime("%Y-%m-%d")
    cnt=db.execute("SELECT COUNT(*) FROM seen WHERE id LIKE ?", (f"{since}%",)).fetchone()[0]
    await c.bot.send_message(CHAT_ID, f"📊 Итоги за сутки: {cnt} очагов")

# ── запуск
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("help", help_cmd))
    jq = app.job_queue
    jq.run_repeating(poll, 60, first=10)
    jq.run_daily(daily, time=datetime.strptime("11:00","%H:%M").time())
    print("[START] bot — only medium/high risks")
    app.run_polling(stop_signals=None)

if __name__ == "__main__":
    main()
