from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import pandas as pd
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Modelo de datos para validación
class CO2Data(BaseModel):
    country: str
    year: int
    commodity: str
    parent_entity: str
    parent_type: str
    value: float

    class Config:
        from_attributes = True

# Inicialización de la aplicación
app = FastAPI(
    title="API de Emisiones de CO2",
    description="API para consultar datos históricos de emisiones de CO2 por países",
    version="1.0.0"
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Carga de datos
try:
    df = pd.read_csv(r'C:\Users\Desti\OneDrive\Escritorio\api_c02\data\df_co2_countrys.csv')
except Exception as e:
    print(f"Error al cargar el archivo CSV: {e}")
    df = pd.DataFrame()  # DataFrame vacío como fallback

# Rutas de la API
@app.get("/", tags=["General"])
async def root():
    """
    Ruta principal para verificar el estado de la API.
    """
    return {
        "status": "online",
        "message": "API de CO2 por países funcionando",
        "version": "1.0.0"
    }

@app.get("/countries/", response_model=List[str], tags=["Países"])
async def get_countries():
    """
    Obtiene la lista de todos los países disponibles en el dataset.
    """
    return df['country'].unique().tolist()

@app.get("/country/{country_name}", response_model=List[CO2Data], tags=["Países"])
async def get_country_data(
    country_name: str,
    year: Optional[int] = Query(None, description="Filtrar por año específico")
):
    """
    Obtiene datos de CO2 para un país específico.
    Opcionalmente se puede filtrar por año.
    """
    query = df['country'].str.lower() == country_name.lower()
    if year:
        query &= (df['year'] == year)
    
    country_data = df[query]
    
    if country_data.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos para el país: {country_name}")
    
    return country_data.to_dict(orient='records')

@app.get("/co2_data/", response_model=List[CO2Data], tags=["CO2"])
async def get_co2_data(
    year: int = Query(..., description="Año para filtrar los datos"),
    country: Optional[str] = Query(None, description="País específico (opcional)")
):
    """
    Obtiene datos de CO2 para un año específico.
    Opcionalmente se puede filtrar por país.
    """
    query = df['year'] == year
    if country:
        query &= (df['country'].str.lower() == country.lower())
    
    data = df[query]
    
    if data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para el año {year}" + 
                   (f" y país {country}" if country else "")
        )
    
    return data.to_dict(orient='records')

@app.get("/column/{column_name}", tags=["Utilidades"])
async def get_column_data(column_name: str):
    """
    Obtiene todos los valores únicos de una columna específica.
    """
    if column_name not in df.columns:
        raise HTTPException(
            status_code=404,
            detail=f"Columna no encontrada. Columnas disponibles: {', '.join(df.columns)}"
        )
    
    return {column_name: df[column_name].dropna().unique().tolist()}

@app.get("/commodity/{commodity_name}", response_model=List[CO2Data], tags=["Commodities"])
async def get_commodity_data(
    commodity_name: str,
    year: Optional[int] = Query(None, description="Filtrar por año específico")
):
    """
    Obtiene datos para un commodity específico.
    Opcionalmente se puede filtrar por año.
    """
    query = df['commodity'].str.lower() == commodity_name.lower()
    if year:
        query &= (df['year'] == year)
    
    commodity_data = df[query]
    
    if commodity_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para el commodity: {commodity_name}"
        )
    
    return commodity_data.to_dict(orient='records')

@app.get("/statistics/summary", tags=["Estadísticas"])
async def get_statistics_summary():
    """
    Obtiene un resumen estadístico de los datos de CO2.
    """
    return {
        "total_countries": len(df['country'].unique()),
        "year_range": {
            "min": df['year'].min(),
            "max": df['year'].max()
        },
        "total_records": len(df),
        "commodities": df['commodity'].unique().tolist(),
        "entity_types": df['parent_type'].unique().tolist()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)