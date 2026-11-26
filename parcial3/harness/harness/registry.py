"""Test seams. Students never touch this; the selftest uses it to inject a simulated AWS."""
AWS_OVERRIDE = None      # object implementing the AWS adapter interface
CONFIG_OVERRIDE = None   # dict replacing config.yaml
THRESHOLDS_OVERRIDE = None


def reset():
    global AWS_OVERRIDE, CONFIG_OVERRIDE, THRESHOLDS_OVERRIDE
    AWS_OVERRIDE = CONFIG_OVERRIDE = THRESHOLDS_OVERRIDE = None
