# Humidity Calculation

This document describes the methods used to calculate relative humidity for ERA5, ERA5-Land, and CMIP6 data.

## ERA5 and ERA5-Land

### `dewpoint_to_relative_humidity`

Calculates two-meter relative humidity from two-meter dewpoint temperature (`d2m`) and two-meter air temperature (`t2m`).

The ECMWF defines near-surface relative humidity as:

$$
RH = 100 \times \frac{e_s(T_d)}{e_s(T)}
$$

where:

- \(T_d\) is the two-meter dewpoint temperature;
- \(T\) is the two-meter air temperature;
- \(e_s\) is the saturation vapor pressure over water.

Using the ECMWF saturation vapor pressure formulation:

$$
e_s(T) =
611.21
\exp\left[
17.502
\frac{T-273.16}{T-32.19}
\right],
$$

the relative humidity is calculated as:

$$
RH =
100
\exp\left[
17.502\frac{T_d-273.16}{T_d-32.19}
-
17.502\frac{T-273.16}{T-32.19}
\right].
$$

The function expects temperatures in kelvin and returns relative humidity in percent. Values may be constrained to the range from 0% to 100%.

#### References

- [ECMWF ERA5 data documentation — Computation of near-surface humidity](https://confluence.ecmwf.int/pages/viewpage.action?navigatingVersions=true&pageId=185086723#ERA5:datadocumentation-Computationofnear-surfacehumidityandsnowcover)
- [ECMWF IFS Documentation CY41R2 — Part IV: Physical Processes](https://www.ecmwf.int/sites/default/files/elibrary/2016/16648-part-iv-physical-processes.pdf), Equation 7.5 and parameters on pages 94–95; equivalent expanded expression in Equation 12.8 on page 193.

## CMIP6

### `specific_to_relative_humidity_standard_pressure`

Calculates relative humidity from near-surface specific humidity (`huss`) and near-surface air temperature (`tas`).

Because surface pressure is not used directly, the calculation assumes the standard atmospheric pressure:

$$
p_0 = 101325\ \text{Pa}.
$$

The actual vapor pressure is calculated from specific humidity as:

$$
e =
\frac{q p_0}
{\varepsilon + (1-\varepsilon)q},
$$

where:

- \(q\) is the specific humidity;
- $\varepsilon = 0.622$;
- \(p_0\) is the assumed atmospheric pressure.

The saturation vapor pressure is calculated using Bolton (1980):

$$
e_s =
611.2
\exp\left(
\frac{17.67T_C}
{T_C+243.5}
\right),
$$

where \(T_C\) is the air temperature in degrees Celsius.

Relative humidity is then calculated as:

$$
RH = 100 \times \frac{e}{e_s}.
$$

The function returns relative humidity in percent. Values may be constrained to the range from 0% to 100%.

Because a constant pressure is assumed, the result is an approximation and may differ from calculations based on the actual surface pressure.

#### References

- [NASA POWER — Relative Humidity Methodology](https://power.larc.nasa.gov/docs/methodology/meteorology/relative-humidity/), Equations 1 and 5.
- [Bolton, D. (1980). The Computation of Equivalent Potential Temperature](https://journals.ametsoc.org/view/journals/mwre/108/7/1520-0493_1980_108_1046_tcoept_2_0_co_2.xml?tab_body=pdf), Equation 10.