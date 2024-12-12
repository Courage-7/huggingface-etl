import unittest
from src.etl import EnhancedHuggingFaceETL

class TestDataValidation(unittest.TestCase):
    def test_model_validation(self):
        etl = EnhancedHuggingFaceETL()
        raw_data = [
            {"modelId": "valid_model", "name": "model1"},
            {"name": "missing_id"},
            {"modelId": "another_valid_model", "description": None}
        ]
        cleaned_data = etl.validate_model_data(raw_data)
        self.assertEqual(len(cleaned_data), 2)
        self.assertNotIn("description", cleaned_data[1])

if __name__ == "__main__":
    unittest.main()
