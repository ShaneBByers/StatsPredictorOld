from DataManager import DataManager
from Logger import Logger
from LinearProgrammingSolver import LinearProgrammingSolver

logger_name = "Predictor_Logger"

logger = Logger(logger_name)

manager = DataManager(logger_name)

manager.predict_lineup()

# manager.current_day_functions()
#
# manager.transfer_active_status()

# manager.get_lp_solution()



# manager.current_day_functions()

# manager.transfer_active_status()



# Look into goalie shutouts
# Linear programming stuff

# Have it work with different slates per day. Not crucial