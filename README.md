# CLEETS-SMART  
**Sustainable Mobility and Resilient Transport**

![Status](https://img.shields.io/badge/status-research--prototype-blue)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Domain](https://img.shields.io/badge/domain-climate--resilient%20transport-orange)

---

CLEETS-SMART  is a dynamic geospatial decision-support platform designed to break down data silos and support transport decarbonisation. It integrates diverse variables into a single interface and embeds optimisation and machine learning algorithms to deliver optimal solutions. 

CLEETS stands for Clean Energy and Equitable Transportation Solutions, while SMART refers to Sustainable Mobility and Resilient Transport. 

<img width="2315" height="1096" alt="Screenshot 2026-04-26 185635" src="https://github.com/user-attachments/assets/974c6ec2-6fd8-44bb-a20b-92ef010a02f6" />


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
  title   = {CLEETS-SMART: A Platform for Climate-Resilient Mobility},
  author  = {Naeima et al.},
  year    = {2026},
  url     = {https://github.com/Naeima/cleets-smart-app}
}
```
---
## Acknowledgements

We acknowledge the Environment Agency, Natural Resources Wales, OpenStreetMap contributors, the Met Office, and the UK Department for Transport for providing open data essential to this work.
