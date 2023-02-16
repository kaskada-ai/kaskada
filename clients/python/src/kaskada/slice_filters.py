from typing import Dict, List, Union


class SliceFilter(object):
    def to_request(self):
        raise NotImplementedError()

    def to_slice_request(self):
        raise NotImplementedError()


class EntityPercentFilter(SliceFilter):
    # The lower limit allowed for percents of entities to filter through (out of a 100)
    # The percent must be greater than or equal to this limit
    LOWER_LIMIT = 0.1
    # The upper limit allowed for percents of entities to filter through (out of a 100).
    # The percent must be less than or equal to this limit
    UPPER_LIMIT = 100.0

    def __init__(self, percent: float):
        """
        Initializes an entity percent filter. Only one filter is allowed per query request.

        Args:
            percent (float): The percent of entities to filter.
                             Must be GTE to LOWER_LIMIT and LTE to UPPER_LIMIT.
        """
        if (
            percent < EntityPercentFilter.LOWER_LIMIT
            or percent > EntityPercentFilter.UPPER_LIMIT
        ):
            raise Exception(
                "percent must be between {} and {}".format(
                    EntityPercentFilter.LOWER_LIMIT, EntityPercentFilter.UPPER_LIMIT
                )
            )
        super().__init__()
        self.percent = percent

    def to_request(self) -> Dict[str, Dict[str, float]]:
        return {"percent": {"percent": self.get_percent()}}

    def get_percent(self) -> float:
        """
        Returns the percent of entities to filter

        Returns:
            float: The percent between (0, 100)
        """
        return self.percent


class EntityFilter(SliceFilter):
    entities: List[str] = []

    def __init__(self, entities: Union[List[int], List[str]]):
        """
        Initializes a filter for a list of entity keys. Only one filter is allowed per query request.
        Although the filter accepts any format, each entity key will be encoded as a string using
        a string formatter.

        Example:
        - Entity Key: "user_id1" encodes to "user_id1"
        - Entity Key: 123 encodes to "123"

        Args:
            entity (str): The entity keys to filter values by.
        """
        self.entities = []
        for e in entities:
            self.entities.append("{}".format(e))

    def to_request(self):
        """Generates the dictionary version of the request

        Returns:
            _type_: _description_
        """
        return {
            "entity_keys": {
                "entity_keys": self.entities,
            }
        }
