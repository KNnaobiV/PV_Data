import sys
import os
current_dir = os.path.dirname(
    os.path.dirname(os.path.dirname(__file__))
    )

sys.path.append(current_dir)
sys.path.append("../..")
print(sys.path)


from vernay.utils import get_model_objects
from vernay.pvoutput.pvoutput.models import (
    Country, Daily, Weekly, Monthly, Yearly, System
)

COUNTRY = get_model_objects_with_relationships(Country, "systems")
SYSTEMS = get_model_objects_with_relationships(
    System, "daily", "weekly", "monthly", "yearly"
)
DAILY = get_model_objects_with_relationships(Daily)
WEEKLY = get_model_objects_with_relationships(Weekly)
MONTHLY = get_model_objects_with_relationships(Monthly)
YEARLY = get_model_objects_with_relationships(Yearly)