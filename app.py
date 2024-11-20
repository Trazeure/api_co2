from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import pandas as pd
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import os

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

# Obtener el puerto de la variable de entorno
PORT = int(os.getenv('PORT', 8000))

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
    df = pd.read_csv('data/df_co2_countrys.csv')
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

# Endpoint para obtener datos por entidad
@app.get("/entity/{entity_name}", response_model=List[CO2Data], tags=["Entidades"])
async def get_entity_data(
    entity_name: str,
    year: Optional[int] = Query(None, description="Filtrar por año específico")
):
    """
    Obtiene datos para una entidad específica.
    Opcionalmente se puede filtrar por año.
    """
    query = df['parent_entity'].str.lower() == entity_name.lower()
    if year:
        query &= (df['year'] == year)
    
    entity_data = df[query]
    
    if entity_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para la entidad: {entity_name}"
        )
    
    return entity_data.to_dict(orient='records')

# Endpoint para obtener datos por tipo de entidad
@app.get("/entity_type/{entity_type}", response_model=List[CO2Data], tags=["Entidades"])
async def get_entity_type_data(
    entity_type: str,
    year: Optional[int] = Query(None, description="Filtrar por año específico")
):
    """
    Obtiene datos para un tipo de entidad específico.
    Opcionalmente se puede filtrar por año.
    """
    query = df['parent_type'].str.lower() == entity_type.lower()
    if year:
        query &= (df['year'] == year)
    
    entity_type_data = df[query]
    
    if entity_type_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para el tipo de entidad: {entity_type}"
        )
    
    return entity_type_data.to_dict(orient='records')

# Endpoint para comparar países
@app.get("/compare_countries/", tags=["Comparaciones"])
async def compare_countries(
    countries: List[str] = Query(..., description="Lista de países a comparar"),
    year: Optional[int] = Query(None, description="Año específico para la comparación")
):
    """
    Compara las emisiones de CO2 entre varios países.
    Opcionalmente se puede especificar un año.
    """
    query = df['country'].str.lower().isin([c.lower() for c in countries])
    if year:
        query &= (df['year'] == year)
    
    comparison_data = df[query]
    
    if comparison_data.empty:
        raise HTTPException(
            status_code=404,
            detail="No se encontraron datos para los países especificados"
        )
    
    return comparison_data.to_dict(orient='records')

# Endpoint para obtener tendencias temporales
@app.get("/trends/{country}", tags=["Tendencias"])
async def get_trends(
    country: str,
    start_year: Optional[int] = Query(None, description="Año inicial"),
    end_year: Optional[int] = Query(None, description="Año final")
):
    """
    Obtiene tendencias temporales de emisiones de CO2 para un país.
    Opcionalmente se puede especificar un rango de años.
    """
    query = df['country'].str.lower() == country.lower()
    if start_year:
        query &= (df['year'] >= start_year)
    if end_year:
        query &= (df['year'] <= end_year)
    
    trend_data = df[query].sort_values('year')
    
    if trend_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron datos para el país: {country}"
        )
    
    return trend_data.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, reload=True)