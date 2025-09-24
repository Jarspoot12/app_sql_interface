import React, { useState, useEffect } from 'react'; // hooks para manejar memoria y efectos respectivamente
import axios from 'axios'; // mensajero para hacer peticiones HTTP al backend 
import { 
  Container, Typography, Box, FormControl, InputLabel, Select, 
  MenuItem, Autocomplete, TextField, Button, CircularProgress, Alert,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper
} from '@mui/material'; // componentes visuales como tablas, botones, etc para no construirlos desde cero

// La URL base de nuestro backend. Debe coincidir con donde corre FastAPI.
const API_URL = 'http://127.0.0.1:8000';

function App() {
  // aquí se define lo que la aplicación necesita recordar, pues al cambiar provoca que la interfaz se actualice automáticamente
  // --- Estados para guardar la información ---
  const [schema, setSchema] = useState({}); // Guardará todas las tablas y columnas
  const [tables, setTables] = useState([]); // Lista de nombres de tablas
  const [columns, setColumns] = useState([]); // Columnas de la tabla seleccionada

  // --- Estados para las selecciones del usuario ---
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [whereClause, setWhereClause] = useState('');

  // --- Estados para los datos y la UI ---
  const [previewData, setPreviewData] = useState([]); // Los datos para la tabla de preview
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // --- Efecto para cargar el esquema de la DB al iniciar la app ---
  useEffect(() => {
    setLoading(true);
    axios.get(`${API_URL}/api/schema`)
      .then(response => {
        setSchema(response.data);
        setTables(Object.keys(response.data));
        setLoading(false);
      })
      .catch(err => {
        setError('No se pudo conectar con el backend.');
        setLoading(false);
        console.error(err);
      });
  }, []); // El array vacío [] significa que esto se ejecuta solo una vez y no se llama a la API en bucle infinito

  // --- Manejador para cuando el usuario selecciona una tabla ---
  const handleTableChange = (event) => {
    const tableName = event.target.value;
    setSelectedTable(tableName);
    setColumns(schema[tableName] || []); // Actualiza las columnas disponibles
    setSelectedColumns([]); // Resetea las columnas seleccionadas
    setPreviewData([]); // Limpia el preview anterior
  };
  
  // --- Manejador para el botón "Mostrar Preview" ---
  const handlePreview = () => {
    if (!selectedTable) {
      setError('Por favor, selecciona una tabla.');
      return;
    }
    setLoading(true);
    setError('');
    setPreviewData([]);
    
    const payload = {
      table: selectedTable,
      columns: selectedColumns.length > 0 ? selectedColumns : []
      // Aquí agregaremos la lógica del WHERE más adelante
    };

    axios.post(`${API_URL}/api/query`, payload) // axios se ejecuta con promesas, se ejecuta .then si tuvo éxito y .catch si falló
      .then(response => {
        setPreviewData(response.data);
        setLoading(false);
      })
      .catch(err => {
        setError('Error al obtener el preview de los datos.');
        setLoading(false);
        console.error(err);
      });
  };

  // --- Manejador para el botón "Descargar Tabla" ---
  const handleDownload = (fileType) => {
    if (!selectedTable) {
      setError('Por favor, selecciona una tabla.');
      return;
    }
    setLoading(true);
    setError('');

    const payload = {
      table: selectedTable,
      columns: selectedColumns.length > 0 ? selectedColumns : [],
      file_type: fileType
    };

    axios.post(`${API_URL}/api/download`, payload, { responseType: 'blob' })
      .then(response => {
        // Crear un link en memoria para descargar el archivo
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;
        const filename = fileType === 'xlsx' ? 'resultado.xlsx' : 'resultado.csv';
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        link.remove();
        setLoading(false);
      })
      .catch(err => {
        setError('Error al descargar el archivo.');
        setLoading(false);
        console.error(err);
      });
  };

  return (
    <Container sx={{ padding: 4 }}>
      <Typography variant="h4" gutterBottom>
        Constructor de Consultas C5
      </Typography>

      {/* --- Sección de Formulario --- */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 4 }}>
        {/* FROM: Selector de Tabla */}
        <FormControl fullWidth>
          <InputLabel>Tabla</InputLabel>
          <Select value={selectedTable} label="FROM (Tabla)" onChange={handleTableChange}>
            {tables.map(table => (
              <MenuItem key={table} value={table}>{table}</MenuItem>
            ))}
          </Select>
        </FormControl>

        {/* SELECT: Selector de Columnas */}
        <Autocomplete
          multiple
          options={columns}
          value={selectedColumns}
          onChange={(event, newValue) => {
            setSelectedColumns(newValue);
          }}
          renderInput={(params) => (
            <TextField {...params} label="Columnas" placeholder="Seleccionar" />
          )}
          disabled={!selectedTable} // Deshabilitado hasta que se elija una tabla
        />

        {/* WHERE: Campo de texto (simple por ahora) */}
        <TextField 
          label="Filtro" 
          variant="outlined"
          value={whereClause}
          onChange={(e) => setWhereClause(e.target.value)}
          placeholder="Ej: id = 10 (lógica futura)"
          disabled={!selectedTable}
        />
      </Box>

      {/* --- Sección de Botones --- */}
      <Box sx={{ display: 'flex', gap: 2, marginBottom: 4 }}>
        <Button variant="contained" onClick={handlePreview} disabled={loading || !selectedTable}>
          {loading ? <CircularProgress size={24} /> : 'Mostrar Preview'}
        </Button>
        <Button variant="outlined" onClick={() => handleDownload('csv')} disabled={loading || !selectedTable}>
          Descargar CSV
        </Button>
        <Button variant="outlined" onClick={() => handleDownload('xlsx')} disabled={loading || !selectedTable}>
          Descargar XLSX
        </Button>
      </Box>

      {/* --- Alerta de Error --- */}
      {error && <Alert severity="error" sx={{ marginBottom: 2 }}>{error}</Alert>}

      {/* --- Tabla de Preview --- */}
      {previewData.length > 0 && (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                {Object.keys(previewData[0]).map(key => (
                  <TableCell key={key}><b>{key}</b></TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {previewData.map((row, index) => (
                <TableRow key={index}>
                  {Object.values(row).map((value, i) => (
                    <TableCell key={i}>{String(value)}</TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Container>
  );
}

export default App;
