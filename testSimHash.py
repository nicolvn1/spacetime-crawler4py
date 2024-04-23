import unittest
import scraper

class TestTraps(unittest.TestCase):
    def test_pos_trap(self):
        link = "https://ics.uci.edu/stay-connected/stay-connected/stay-connected/stay-connected/stay-connected/stay-connected/"
        result = scraper.pos_trap(link)
        self.assertTrue(result)
