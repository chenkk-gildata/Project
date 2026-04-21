import contextlib
import io
import unittest
from unittest.mock import patch

from pdf_processor import PDFProcessor


class PDFProcessorOutputTests(unittest.TestCase):
    def test_processor_does_not_keep_search_debug_helpers(self):
        processor = PDFProcessor()

        self.assertFalse(hasattr(processor, "_search_debug_counter"))
        self.assertFalse(hasattr(processor, "save_search_debug_info"))

    def test_process_batch_prints_only_summary_counts(self):
        processor = PDFProcessor()

        def fake_process_single_pdf(self, pdf_path):
            with self.lock:
                self.processed_count += 1
            mapping = {
                "a.pdf": "completed",
                "b.pdf": "special_processed",
                "c.pdf": "skipped",
                "d.pdf": "failed",
            }
            return mapping[pdf_path]

        buffer = io.StringIO()
        with patch.object(PDFProcessor, "process_single_pdf", fake_process_single_pdf):
            with contextlib.redirect_stdout(buffer):
                success_count, failed_count = processor.process_batch(
                    ["a.pdf", "b.pdf", "c.pdf", "d.pdf"]
                )

        output = buffer.getvalue()
        self.assertEqual(success_count, 2)
        self.assertEqual(failed_count, 1)
        self.assertIn("处理完成: 1", output)
        self.assertIn("特殊处理: 1", output)
        self.assertIn("跳过处理: 1", output)
        self.assertIn("处理失败: 1", output)
        self.assertNotIn("搜索股东背景介绍范围", output)

    def test_process_single_pdf_skip_does_not_print_details(self):
        processor = PDFProcessor()
        processor.is_processing = True

        buffer = io.StringIO()
        with patch.object(processor, "get_exchange_code", return_value="kcb"):
            with contextlib.redirect_stdout(buffer):
                result = processor.process_single_pdf("demo.pdf")

        self.assertEqual(result, "skipped")
        self.assertEqual("", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
