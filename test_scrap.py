import unittest
import scraper

class TestTraps(unittest.TestCase):
    def test_pos_trap_true(self):
        link = "https://ics.uci.edu/stay-connected/stay-connected/stay-connected/stay-connected/stay-connected/stay-connected/"
        result = scraper.pos_trap(link)
        self.assertTrue(result, "Staying connected forever!")
        
    def test_pos_trap_false(self):
        link = "https://ics.uci.edu/events/category/alumni-events/list/?tribe-bar-date=2024-04-24"
        result = scraper.pos_trap(link)
        self.assertFalse(result, "nothing worng with this path...")
    
    def test_pos_calendar_true1(self):
        link = "https://ics.uci.edu/events/category/alumni-events/list/?tribe-bar-date=2024-04-24"
        result = scraper.pos_calendar(link)
        self.assertTrue(result, "did not detect date in query")
    
    def test_pos_calendar_true2(self):
        link = "https://ics.uci.edu/events/category/alumni-events/2024-02/"
        result = scraper.pos_calendar(link)
        self.assertTrue(result, "did not detect date in path")
