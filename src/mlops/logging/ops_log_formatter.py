import logging


class OpsLogFormatter(logging.Formatter):
    """
        The OpsLogFormatter class is a custom formatter designed to enhance Python's standard logging.
        Formatter for specific logging requirements. Its primary function is to extract and process additional arguments
        from log records, making them accessible for formatting.
    """

    def format(self, record):
        """
            Overrides the base class' format method to handle specific logic:
                Checks if record.args is a dictionary.
                If it is, extracts additional_arguments from the dictionary and assigns it to the record object.
                If additional_arguments is not found, creates an empty dictionary.
                Calls the parent class' format method to perform standard formatting.
        """
        if 'dict' in str(type(record.args)):
            additional_arguments = record.args.get("additional_arguments", None)
            record.additional_arguments = additional_arguments if additional_arguments is not None else {}
        else:
            record.additional_arguments = {}
        return super().format(record)
