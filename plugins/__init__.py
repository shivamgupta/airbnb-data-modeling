from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class AirbnbPlugin(AirflowPlugin):
    name = "airbnb_plugin"
    operators = [
        operators.CheckSourceOperator,
        operators.CleanSourceOperator,
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
