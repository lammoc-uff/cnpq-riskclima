# CNPq RiskClima

Project website: ``https://riskclima.com.br``

This repository organizes the main computational components of the ``RiskClima`` project.

## What the project does

RiskClima is a project focused on the development of georeferenced maps that classify the areas of Brazil most vulnerable to climate extremes. The project also identifies the type of event associated with each region and the corresponding social, environmental, or socioenvironmental vulnerability.

The project integrates climate modeling, CMIP6-based scenarios, regionalized climate analysis, hydrological and geomorphological applications, and artificial intelligence methods to support climate risk assessment in Brazil.

Current components in this repository include:

- ``cmip6-downloader`` — workflow for catalog comparison, filtering, download, preprocessing, and ensemble generation of CMIP6 datasets
- ``index-xhwi`` — reference to the dedicated repository of the Extreme Heatwave Index

## Why the project is useful

RiskClima supports the generation of climate risk products relevant to droughts, floods, inundation, mass movements, heatwaves and human health impacts.

The project combines climate model outputs with socioenvironmental indicators, population density, and also evaluates interactions between extreme events and Brazilian biomes. The project further considers associations with chronic non-communicable diseases using health-related datasets.

## How users can get started with the project

### Repository structure

    cnpq-riskclima/
    ├── cmip6-downloader/
    └── index-xhwi/

### Current components

- ``cmip6-downloader``  
  Tools for comparing AWS and Google CMIP6 catalogs, filtering target datasets, downloading selected members, preprocessing datasets, saving Zarr outputs, and building ensembles.

- ``index-xhwi``  
  Reference directory pointing to the dedicated XHWI repository used in the RiskClima workflow.

### Suggested workflow

1. Use ``cmip6-downloader`` to identify, filter, and prepare the climate model datasets required by the project.
2. Use the dedicated ``XHWI`` repository to compute the Extreme Heatwave Index.
3. Integrate outputs with other RiskClima components for hazard, vulnerability, and risk mapping.

## Where users can get help with your project

Users can get help through:

- the official RiskClima website
- the maintainers of each project component
- the documentation available inside each subproject
- direct contact with the project coordination

## Who maintains and contributes to the project

RiskClima is developed by a multidisciplinary team working on climate modeling, hydrology, socioenvironmental vulnerability, risk analysis, and applied data science.

The project is associated with ``LAMMOC/UFF`` and ``COPPE/UFRJ``, and uses climate scenarios through selected CMIP6 models. The project website also presents a team page and institutional support and funding sections.

## Project scope

According to the official project description, RiskClima aims to produce georeferenced vulnerability maps for Brazil, using climate scenarios, hydrological modeling, landslide and coastal erosion modeling, and artificial intelligence techniques. The project focuses on regions susceptible to prolonged droughts, floods, inundation, mass movements, heatwaves and health impacts.

## Official contact

- Email: ``mcataldi@id.uff.br``
- Phone: ``(21) 2629-7604``

## Website

- ``https://riskclima.com.br``
