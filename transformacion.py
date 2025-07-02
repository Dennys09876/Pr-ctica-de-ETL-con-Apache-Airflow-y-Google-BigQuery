import pandas as pd

def transformar_datos(input_path, output_path):
    """
    Lee un archivo CSV, aplica transformaciones y guarda el resultado limpio.
    :param input_path: Ruta al archivo CSV de entrada
    :param output_path: Ruta al archivo CSV de salida limpio
    """
    try:
        # Leer el archivo de entrada
        df = pd.read_csv(input_path)

        # Eliminar filas completamente vacías
        df.dropna(how='all', inplace=True)

        # Llenar valores nulos en campos críticos
        df.fillna({
            'Sector': 'No definido',
            'Actividad': 'No especificada',
            'Variacion_Mensual': 0.0,
            'Anio': 0,
            'Mes': 0
        }, inplace=True)

        # Convertir a tipos apropiados
        df['Variacion_Mensual'] = pd.to_numeric(df['Variacion_Mensual'], errors='coerce')
        df['Anio'] = df['Anio'].astype(int)
        df['Mes'] = df['Mes'].astype(int)

        # Crear columna de periodo
        df['Periodo_Anio_Mes'] = df['Anio'].astype(str) + df['Mes'].astype(str).str.zfill(2)

        # Filtrar registros válidos
        df = df[df['Anio'] > 2019]  # Por ejemplo, solo años recientes

        # Guardar archivo limpio
        df.to_csv(output_path, index=False)
        print(f"Archivo transformado guardado en: {output_path}")

    except Exception as e:
        print(f"Error al transformar datos: {e}")

# Ejecución directa para pruebas
if __name__ == "__main__":
    transformar_datos("data/sectores_economicos.csv", "data/sectores_economicos_limpio.csv")
