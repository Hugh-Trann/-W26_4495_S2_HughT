# W26\_4495\_S2\_HughT



Student name: Hugh Tran
SID: 300394597
Email: tranq18@student.douglascollege.ca



Project name: "IBP Project - Intelligent Business Performance Analytics and Decision Support Platform For Small Business"



The platform combines data ingestion, analysis, forecasting, recommendations, reporting and user interaction. The expected benefits include better decision-making, earlier identification of performance problems, and a reusable analytics framework that can be applied to different environment.



Initial setups:

* Code editor: Visual Studio Code
* Database management system: SQL Server



Steps to run the demo from VSC's terminal:

1. Create a virtual environment: python -m venv venv
2. Activate the virtual environment: .\\.venv\\Scripts\\Activate.ps1
3. Install python packages: pip install pandas polars pyarrow scikit-learn streamlit  matplotlib seaborn uvicorn fastapi pyodbc python-multipart sqlalchemy
4. Generate dependencies file: pip freeze > requirements.txt
5. Install dependencies: pip install -r front\_end\\requirements.txt
6. Start FastAPI application: uvicorn front\_end.main:app
7. Access the platform: http://127.0.0.1:8000
8. Sample data for uploading: ...\\W26\_4495\_S2\_HughT\\Implementation\\data\\grocery store
