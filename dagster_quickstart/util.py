from pint import UnitRegistry


def get_standard_quantity(quantity_string, standard_unit):
    ureg = UnitRegistry()

    # Handle unit variants
    ureg.define("Mi = 1 * MiB")
    ureg.define("millicore = 1 * m")

    qty = ureg(quantity_string)   
            
    return qty.to(standard_unit).magnitude