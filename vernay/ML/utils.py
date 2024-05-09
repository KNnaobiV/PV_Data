import sys
import os
current_dir = os.path.dirname(os.path.dirname(__file__))

sys.path.append(current_dir)
sys.path.append("..",)
print(sys.path)


from vernay.utils import get_model_objects
from vernay.pvoutput.pvoutput.models import (
    Country, Daily, Weekly, Monthly, Yearly, System
)

DAILY = get_model_objects(Daily)