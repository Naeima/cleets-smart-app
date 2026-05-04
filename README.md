# CLEETS-SMART  
**Sustainable Mobility and Resilient Transport**

![Status](https://img.shields.io/badge/status-research--prototype-blue)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Domain](https://img.shields.io/badge/domain-climate--resilient%20transport-orange)

---

CLEETS-SMART  is a dynamic geospatial decision-support platform designed to break down data silos and support transport decarbonisation. It integrates diverse variables into a single interface and embeds optimisation and machine learning algorithms to deliver optimal solutions. 

CLEETS stands for Clean Energy and Equitable Transportation Solutions, while SMART refers to Sustainable Mobility and Resilient Transport. 

Technically, CLEETS-SMART is a modular and scalable Python dashboard that supports transport, weather, flood-risk, and EV-planning decisions for UK regions by integrating live APIs, tabular datasets, spatial layers, and interactive map/chart interfaces.

<img width="857" height="283" alt="image" src="https://github.com/user-attachments/assets/6020dfd0-2437-4476-9a9c-b9fb686ea8be" />

<img width="854" height="326" alt="image" src="https://github.com/user-attachments/assets/e81c7620-0aa7-4afe-b0d9-b5b4fb74e5f2" />




## System Architecture

1. Data Ingestion Layer  
2. Data Integration & Processing  
3. Risk Modelling Layer  
4. Routing & Optimisation Layer  
5. Application Layer  

---

## Methodology

Routing is formulated as a Resource Constrained Shortest Path (RCSP) problem with:

- Battery constraints  
- State-of-Charge (SoC) thresholds  
- Risk penalties applied to hazardous edges  

---

## Data Sources

- Environment Agency — https://environment.data.gov.uk/  
- Natural Resources Wales — https://datamap.gov.wales/  
- OpenStreetMap — https://www.openstreetmap.org/  
- OSRM — http://project-osrm.org/  
- UK Department for Transport — https://www.gov.uk/  
- Met Office — https://www.metoffice.gov.uk/  
- UK Climate Projections — https://www.metoffice.gov.uk/research/approach/collaboration/ukcp
- DAFNI https://dafni.rl.ac.uk/ 
---

## Reproducibility

```bash
git clone https://github.com/Naeima/cleets-smart-app.git
cd cleets-smart-app
pip install -r requirements.txt
python app.py
```

---

## Citation

```bibtex
@software{cleets_smart_2026,
  title   = {CLEETS-SMART: Sustainable Mobility and Resilient Transport},
  author  = {Naeima et al.},
  year    = {2026},
  url     = {https://github.com/Naeima/cleets-smart-app}
}
```
---
## Acknowledgements

We acknowledge the Environment Agency, Natural Resources Wales, OpenStreetMap contributors, the Met Office, DAFNI, and the UK Department for Transport for providing open data essential to this work.
