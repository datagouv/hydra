import pandas as pd

NA_VALUES = pd._libs.parsers.STR_NA_VALUES | {
    "Non spécifié",
    "NC",
}
