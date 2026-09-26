"""Constants for combustion."""
from logging import Logger, getLogger

LOGGER: Logger = getLogger(__package__)

NAME = "Combustion"
DOMAIN = "combustion"
MANUFACTURER = "Combustion, Inc."
DEVICE_NAME = "Predictive Thermometer"
VERSION = "0.0.0"
ATTRIBUTION = ""

BT_MANUFACTURER_ID = 2503

CONF_DEVICES = "devices"

PRODUCT_TYPE_PROBE = 1
PRODUCT_TYPE_REPEATER_NODE = 2

CONF_AVAILABILITY_TIMEOUT = "availability_timeout"
CONF_UPDATE_THROTTLE = "update_throttle"
CONF_ENABLE_ACTIVE_CONNECTION = "enable_active_connection"

# Optional MeatNet Cloud link. These values stay in config-entry data, not
# options: changing credentials/account identity is a reconfigure/reauth action.
CONF_CLOUD_API_KEY = "cloud_api_key"
CONF_CLOUD_REFRESH_TOKEN = "cloud_refresh_token"
CONF_CLOUD_SUBJECT = "cloud_subject"
CONF_CLOUD_LINK_GENERATION = "cloud_link_generation"


DEFAULT_AVAILABILITY_TIMEOUT = 90
DEFAULT_UPDATE_THROTTLE = 1.0
DEFAULT_ENABLE_ACTIVE_CONNECTION = False
