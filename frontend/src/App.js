import React, {useState, useEffect} from 'react';
import axios from 'axios';
import { 
  Container, Typography, Box, FormControl, InputLabel, Select, 
  MenuItem, Autocomplete, TextField, Button, CircularProgress, Alert,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper
} from '@mui/material';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import dayjs from 'dayjs';

const API_URL = 'http://127.0.0.1:8000';

function App() {
  const [schema, setSchema] = useState({});
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [filters, setFilters] = useState([]);

  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState([]);

  const [previewData, setPreviewData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    setLoading(true);
    axios.get(`${API_URL}/api/schema`)
      .then(response => {
        setSchema(response.data);
        setTables(Object.keys(response.data));
        setLoading(false);
      })
      .catch(err => {
        setError('No se pudo conectar con el backend. ¿Está encendido?');
        setLoading(false);
        console.error(err);
      });
  }, []);

  const handleTableChange = (event) => {
    const tableName = event.target.value;
    setSelectedTable(tableName);
    setColumns(schema[tableName] || []);
    setSelectedColumns([]);
    setFilters([]);
    setPreviewData([]);
  };
  
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
      columns: selectedColumns.map(c => c.column_name),
      filters: filters.filter(f => f.column && f.value) 
    };

    axios.post(`${API_URL}/api/query`, payload)
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

  const handleDownload = (fileType) => {
    if (!selectedTable) {
      setError('Por favor, selecciona una tabla.');
      return;
    }
    setLoading(true);
    setError('');

    const payload = {
      table: selectedTable,
      columns: selectedColumns.map(c => c.column_name),
      filters: filters.filter(f => f.column && f.value),
      file_type: fileType
    };

    axios.post(`${API_URL}/api/download`, payload, { responseType: 'blob' })
      .then(response => {
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

  const addFilter = () => {
    setFilters([...filters, { id: Date.now(), column: '', operator: '=', value: '', logical: 'AND' }]);
  };

  const removeFilter = (id) => {
    setFilters(filters.filter(f => f.id !== id));
  };

  const handleFilterChange = (id, field, value) => {
    const updatedFilters = filters.map(f => (f.id === id ? { ...f, [field]: value } : f));
    setFilters(updatedFilters);
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Container sx={{ padding: 4 }}>
        <Typography variant="h4" gutterBottom>
          Constructor de Consultas SQL
        </Typography>

        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 4 }}>
          <FormControl fullWidth>
            <InputLabel>Tabla</InputLabel>
            <Select value={selectedTable} label="Tabla" onChange={handleTableChange}>
              {tables.map(table => (
                <MenuItem key={table} value={table}>{table}</MenuItem>
              ))}
            </Select>
          </FormControl>

          <Autocomplete
            multiple
            options={columns}
            getOptionLabel={(option) => option.column_name}
            value={selectedColumns}
            onChange={(event, newValue) => {
              setSelectedColumns(newValue);
            }}
            renderInput={(params) => (
              <TextField {...params} label="Columnas" placeholder="Todas" />
            )}
            disabled={!selectedTable}
          />

          <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>
            Filtros (WHERE)
          </Typography>

          {filters.map((filter, index) => (
            <Box key={filter.id} sx={{ display: 'flex', gap: 2, alignItems: 'center', mb: 2 }}>
              
              {index > 0 && (
                <FormControl sx={{ minWidth: 100 }}>
                  <Select value={filter.logical} onChange={(e) => handleFilterChange(filter.id, 'logical', e.target.value)}>
                    <MenuItem value="AND">Y (AND)</MenuItem>
                    <MenuItem value="OR">O (OR)</MenuItem>
                  </Select>
                </FormControl>
              )}

              <FormControl fullWidth>
                <InputLabel>Columna</InputLabel>
                <Select value={filter.column} onChange={(e) => handleFilterChange(filter.id, 'column', e.target.value)}>
                  {columns.map(col => (
                    <MenuItem key={col.column_name} value={col.column_name}>
                      {col.column_name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <FormControl sx={{ minWidth: 180 }}>
                <InputLabel>Operador</InputLabel>
                <Select value={filter.operator} onChange={(e) => handleFilterChange(filter.id, 'operator', e.target.value)}>
                  <MenuItem value="=">Igual a (=)</MenuItem>
                  <MenuItem value="!=">Diferente de (!=)</MenuItem>
                  {/* CORRECCIÓN: Usar entidades HTML &lt; y &gt; para los símbolos */}
                  <MenuItem value=">">Mayor que (&gt;)</MenuItem>
                  <MenuItem value=">=">Mayor o igual que (&gt;=)</MenuItem>
                  <MenuItem value="<">Menor que (&lt;)</MenuItem>
                  <MenuItem value="<=">Menor o igual que (&lt;=)</MenuItem>
                  <MenuItem value="startswith">Inicia con</MenuItem>
                  <MenuItem value="endswith">Termina con</MenuItem>
                  <MenuItem value="contains">Contiene</MenuItem>
                </Select>
              </FormControl>
              
              {(() => {
                const selectedColumnData = columns.find(c => c.column_name === filter.column);
                const dataType = selectedColumnData ? selectedColumnData.data_type : 'text';

                if (dataType.includes('date') || dataType.includes('timestamp')) {
                  return (
                    <DatePicker
                      label="Valor"
                      value={filter.value ? dayjs(filter.value) : null}
                      onChange={(newValue) => handleFilterChange(filter.id, 'value', newValue ? newValue.format('YYYY-MM-DD') : '')}
                      renderInput={(params) => <TextField {...params} />}
                      sx={{ minWidth: 200 }}
                    />
                  );
                } else if (dataType.includes('int') || dataType.includes('numeric') || dataType.includes('double')) {
                  return <TextField label="Valor" type="number" value={filter.value} onChange={(e) => handleFilterChange(filter.id, 'value', e.target.value)} sx={{ minWidth: 200 }} />;
                } else {
                  return <TextField label="Valor" type="text" value={filter.value} onChange={(e) => handleFilterChange(filter.id, 'value', e.target.value)} sx={{ minWidth: 200 }} />;
                }
              })()}
              
              <Button onClick={() => removeFilter(filter.id)} color="error">X</Button>
            </Box>
          ))}

          <Button onClick={addFilter} disabled={!selectedTable}>+ Añadir Filtro</Button>
        </Box>

        <Box sx={{ display: 'flex', gap: 2, marginBottom: 4 }}>
          <Button variant="contained" onClick={handlePreview} disabled={loading || !selectedTable}>
            {loading ? <CircularProgress size={24} /> : 'Mostrar Preview'}
          </Button>
          <Button variant="outlined" onClick={() => handleDownload('csv')} disabled={loading || !selectedTable}>Descargar CSV</Button>
          <Button variant="outlined" onClick={() => handleDownload('xlsx')} disabled={loading || !selectedTable}>Descargar XLSX</Button>
        </Box>

        {error && <Alert severity="error" sx={{ marginBottom: 2 }}>{error}</Alert>}

        {previewData.length > 0 && (
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>{Object.keys(previewData[0]).map(key => <TableCell key={key}><b>{key}</b></TableCell>)}</TableRow>
              </TableHead>
              <TableBody>
                {previewData.map((row, index) => (
                  <TableRow key={index}>{Object.values(row).map((value, i) => <TableCell key={i}>{String(value)}</TableCell>)}</TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Container>
    </LocalizationProvider>
  );
}

export default App;
