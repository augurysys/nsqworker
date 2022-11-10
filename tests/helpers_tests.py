import unittest

from nsqworker.helpers import check_if_not_in_flight


class HelpersTests(unittest.TestCase):

    def test_check_if_not_in_flight(self):
        not_in_flight_error1 = Exception(b'E_TOUCH_FAILED TOUCH 1191daa35f6d9000 failed ID not in flight')
        self.assertTrue(check_if_not_in_flight(not_in_flight_error1))
        not_in_flight_error2 = Exception('E_TOUCH_FAILED TOUCH 1191daa35f6d9000 failed ID not in flight')
        self.assertTrue(check_if_not_in_flight(not_in_flight_error2))
        other_error = Exception('Some other error')
        self.assertFalse(check_if_not_in_flight(other_error))

