import json
import time
import random
from datetime import datetime, timedelta

from kafka import KafkaProducer
from trino.dbapi import connect
from basketball_reference_web_scraper import client


class NBABoxscoreProducer:
    KAFKA_SERVER = "kafka:9092"
    TOPIC = "topic_nba_boxscore"
    DEFAULT_START_DATE = datetime(2025, 10, 1)

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[self.KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.conn = connect(host="trino", port=8080, user="admin", catalog="iceberg", schema="nba")

    def _get_last_date(self) -> datetime:
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT MAX(fecha_partido) FROM boxscores_api")
            res = cur.fetchone()
            if res and res[0]:
                print(f"✅ Última fecha en Iceberg: {res[0]}")
                return datetime.strptime(res[0], "%Y-%m-%d")
        except Exception as e:
            print(f"⚠️ No se pudo obtener la última fecha: {e}")
        print(f"⚠️ Tabla vacía. Iniciando carga masiva desde {self.DEFAULT_START_DATE.date()}.")
        return self.DEFAULT_START_DATE

    def _fetch_boxscores(self, date: datetime) -> list[dict]:
        try:
            return client.player_box_scores(
                day=date.day,
                month=date.month,
                year=date.year
            )
        except Exception as e:
            print(f"   ❌ Error obteniendo box scores para {date.date()}: {e}")
            return []

    def _build_record(self, player: dict, date: datetime) -> dict:
        def enum_name(val):
            return val.value if hasattr(val, "value") else str(val)

        return {
            "slug": str(player.get("slug", "")),
            "name": str(player.get("name", "")),
            "team": enum_name(player.get("team", "")),
            "location": enum_name(player.get("location", "")),
            "opponent": enum_name(player.get("opponent", "")),
            "outcome": enum_name(player.get("outcome", "")),
            "seconds_played": int(player.get("seconds_played", 0) or 0),
            "made_field_goals": int(player.get("made_field_goals", 0) or 0),
            "attempted_field_goals": int(player.get("attempted_field_goals", 0) or 0),
            "made_three_point_field_goals": int(player.get("made_three_point_field_goals", 0) or 0),
            "attempted_three_point_field_goals": int(player.get("attempted_three_point_field_goals", 0) or 0),
            "made_free_throws": int(player.get("made_free_throws", 0) or 0),
            "attempted_free_throws": int(player.get("attempted_free_throws", 0) or 0),
            "offensive_rebounds": int(player.get("offensive_rebounds", 0) or 0),
            "defensive_rebounds": int(player.get("defensive_rebounds", 0) or 0),
            "total_rebounds": int(player.get("total_rebounds", 0) or 0),
            "assists": int(player.get("assists", 0) or 0),
            "steals": int(player.get("steals", 0) or 0),
            "blocks": int(player.get("blocks", 0) or 0),
            "turnovers": int(player.get("turnovers", 0) or 0),
            "personal_fouls": int(player.get("personal_fouls", 0) or 0),
            "game_score": float(player.get("game_score", 0.0) or 0.0),
            "points": int(player.get("points", 0) or 0),
            "fecha_partido": date.strftime("%Y-%m-%d")
        }

    def _process_date(self, date: datetime):
        print(f"   -> Procesando {date.date()}...")
        players = self._fetch_boxscores(date)
        if not players:
            print(f"      ⚠️ Sin datos para {date.date()}, puede ser día sin partidos.")
            return
        for player in players:
            self.producer.send(self.TOPIC, self._build_record(player, date))
        self.producer.flush()
        print(f"      ✅ {len(players)} registros enviados.")
        time.sleep(random.uniform(3.0, 5.0))

    def run(self):
        since = self._get_last_date() + timedelta(days=1)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        if since >= today:
            print("✅ Los datos ya están al día.")
            return

        print(f"\n🚀 Iniciando ingesta desde {since.date()} hasta {(today - timedelta(days=1)).date()}...")
        current = since
        while current < today:
            self._process_date(current)
            current += timedelta(days=1)

        print("\n✅ ¡Ingesta completada con éxito!")


if __name__ == "__main__":
    NBABoxscoreProducer().run()