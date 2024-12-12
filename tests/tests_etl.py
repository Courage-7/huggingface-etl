import unittest
from src.etl import EnhancedHuggingFaceETL


class TestETL(unittest.TestCase):
    def setUp(self):
        self.etl = EnhancedHuggingFaceETL()

    def test_extraction(self):
        models = self.etl.extract_models(max_models=10)
        self.assertTrue(len(models) > 0)

    def test_validation(self):
        raw_models = [{"modelId": "test1"}, {"invalid_field": "test"}]
        cleaned = self.etl.validate_model_data(raw_models)
        self.assertEqual(len(cleaned), 1)


if __name__ == "__main__":
    unittest.main()
