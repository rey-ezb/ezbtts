import importlib.util
import unittest
from pathlib import Path

import pandas as pd


SERVER_PATH = Path(__file__).resolve().parent / "server.py"
spec = importlib.util.spec_from_file_location("dashboard_server", SERVER_PATH)
server = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(server)


def sample_sheet_df():
    rows = [
        ["Date", "TikTok", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        [None, "Birria", None, None, "Pozole", None, None, "Tinga", None, None, "Brine", None, None, "Variety Pack", None, None, "Pozole Verde", None, None],
        [None, "In Transit", "On Hand", "$", "In Transit", "On Hand", "$", "In Transit", "On Hand", "$", "In Transit", "On Hand", "$", "In Transit", "On Hand", "$", "In Transit", "On Hand", "$"],
        ["4/11/2026", "10,080", "14,131", "$43,806", "0", "5,944", "$18,129", "0", "2,606", "$8,209", "0", "3222", "$13,532", "0", "868", "$11,588", "0", "1,697", "$6,364"],
        ["4/12/2026", "10,080", "13,705", "$42,486", "0", "5,901", "$17,998", "0", "2,601", "$8,193", "0", "3222", "$13,532", "0", "867", "$11,574", "0", "1,529", "$5,734"],
        ["4/13/2026", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
    ]
    return pd.DataFrame(rows)


class InventorySheetTests(unittest.TestCase):
    def test_parses_tiktok_inventory_history(self):
        history = server.build_tiktok_inventory_history_from_sheet(sample_sheet_df())
        self.assertEqual(
            list(history.columns),
            [
                "date",
                "birria_in_transit",
                "birria_on_hand",
                "pozole_in_transit",
                "pozole_on_hand",
                "tinga_in_transit",
                "tinga_on_hand",
                "brine_in_transit",
                "brine_on_hand",
                "variety_pack_in_transit",
                "variety_pack_on_hand",
                "pozole_verde_in_transit",
                "pozole_verde_on_hand",
                "valid_metric_count",
            ],
        )
        self.assertEqual(len(history), 3)
        self.assertEqual(int(history.iloc[1]["birria_on_hand"]), 13705)
        self.assertEqual(int(history.iloc[1]["pozole_verde_on_hand"]), 1529)

    def test_uses_latest_row_with_actual_tiktok_data(self):
        history = server.build_tiktok_inventory_history_from_sheet(sample_sheet_df())
        snapshot = server.latest_tiktok_inventory_snapshot(history)
        self.assertEqual(snapshot["snapshot_date"].strftime("%Y-%m-%d"), "2026-04-12")
        self.assertEqual(snapshot["valid_metric_count"], 12)
        self.assertEqual(snapshot["products"]["Birria"]["on_hand"], 13705.0)
        self.assertEqual(snapshot["products"]["Birria"]["in_transit"], 10080.0)


if __name__ == "__main__":
    unittest.main()
