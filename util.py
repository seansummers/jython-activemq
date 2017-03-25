import java.util


def load_properties(property_filename):
    properties = java.util.Properties()
    with open(property_filename, 'r') as property_file:
        properties.load(property_file)
    return properties
