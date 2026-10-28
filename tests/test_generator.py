import json, tempfile, unittest
from pathlib import Path
from src.generate_data import generate

class GeneratorTests(unittest.TestCase):
    def test_starter_counts_and_validation(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/"out"
            summary=generate(out,"starter",20261022)
            self.assertEqual(summary["counts"]["Shipment"],20)
            self.assertEqual(summary["counts"]["Container"],10)
            self.assertEqual(summary["validation_status"],"PASS_WITH_LABELLED_ANOMALIES")
            self.assertTrue((out/"ground_truth.csv").exists())
            self.assertTrue((out/"coldchain.sqlite").exists())

if __name__=="__main__": unittest.main()
