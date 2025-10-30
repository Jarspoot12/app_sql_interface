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
  const [totalCount, setTotalCount] = useState(0);

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
        setPreviewData(response.data.previewData || []);
        setTotalCount(response.data.totalCount || 0); setLoading(false);
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
    const updatedFilters = filters.map(f => {
      if (f.id === id) {
        const newFilter = { ...f, [field]: value };
        // Si el usuario cambia de operador, reseteamos el valor para evitar inconsistencias
        // Especialmente al cambiar a/desde 'between'
        if (field === 'operator') {
          newFilter.value = (value === 'between') ? ['', ''] : '';
        }
        return newFilter;
      }
      return f;
    });
    setFilters(updatedFilters);
  };

  const handleBetweenValueChange = (id, index, value) => {
    const updatedFilters = filters.map(f => {
      if (f.id === id) {
        const newValues = [...f.value];
        newValues[index] = value;
        return { ...f, value: newValues };
      }
      return f;
    });
    setFilters(updatedFilters);
  };


  const operatorMap = {
    numeric: [
      { value: '=', label: 'Igual a (=)' },
      { value: '!=', label: 'Diferente de (!=)' },
      { value: '>', label: 'Mayor que (&gt;)' },
      { value: '<', label: 'Menor que (&lt;)' },
      { value: '>=', label: 'Mayor o igual que (&gt;=)' },
      { value: '<=', label: 'Menor o igual que (&lt;=)' },
      { value: 'between', label: 'Entre (Between)' },
    ],
    text: [
      { value: '=', label: 'Igual a (=)' },
      { value: '!=', label: 'Diferente de (!=)' },
      { value: 'startswith', label: 'Inicia con' },
      { value: 'endswith', label: 'Termina con' },
      { value: 'contains', label: 'Contiene' },
    ],
    date: [
      { value: '=', label: 'Igual a (=)' },
      { value: '!=', label: 'Diferente de (!=)' },
      { value: '>', label: 'Mayor que (&gt;)' },
      { value: '<', label: 'Menor que (&lt;)' },
      { value: '>=', label: 'Mayor o igual que (&gt;=)' },
      { value: '<=', label: 'Menor o igual que (&lt;=)' },
      { value: 'between', label: 'Entre (Between)' },
    ],
  };

  const getDataTypeCategory = (dataType) => {
    if (!dataType) return 'text';
    if (dataType.includes('int') || dataType.includes('numeric') || dataType.includes('double')) return 'numeric';
    if (dataType.includes('date') || dataType.includes('timestamp')) return 'date';
    return 'text';
  };

return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Container sx={{ padding: 4 }}>
        <Typography variant="h4" gutterBottom>
          Constructor de Consultas SQL
        </Typography>

        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 4 }}>
          {/* --- SELECTOR DE TABLA --- */}
          <FormControl fullWidth>
            <InputLabel>Tabla</InputLabel>
            <Select value={selectedTable} label="Tabla" onChange={handleTableChange}>
              {tables.map(table => (
                <MenuItem key={table} value={table}>{table}</MenuItem>
              ))}
            </Select>
          </FormControl>

          {/* --- SELECTOR DE COLUMNAS  --- */}
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

          {/* ================================================================== */}
          {/* INICIO DE LA SECCIÓN MODIFICADA: RENDERIZADO DE FILTROS           */}
          {/* ================================================================== */}
          {filters.map((filter, index) => {
            // --- Lógica que se ejecuta para CADA filtro ANTES de dibujarlo ---
            const selectedColumnData = columns.find(c => c.column_name === filter.column);
            const dataTypeCategory = getDataTypeCategory(selectedColumnData?.data_type);
            const availableOperators = operatorMap[dataTypeCategory] || operatorMap.text;

            // --- Estructura visual que se retorna para CADA filtro ---
            return (
              <Box key={filter.id} sx={{ display: 'flex', gap: 2, alignItems: 'center', mb: 2 }}>
                
                {/* Conector Lógico (AND/OR) */}
                {index > 0 && (
                  <FormControl sx={{ minWidth: 100 }}>
                    <Select value={filter.logical} onChange={(e) => handleFilterChange(filter.id, 'logical', e.target.value)}>
                      <MenuItem value="AND">Y (AND)</MenuItem>
                      <MenuItem value="OR">O (OR)</MenuItem>
                    </Select>
                  </FormControl>
                )}

                {/* Selector de Columna */}
                <FormControl sx={{ minWidth: 180 }}>
                  <InputLabel>Columna</InputLabel>
                  <Select value={filter.column} onChange={(e) => handleFilterChange(filter.id, 'column', e.target.value)}>
                    {columns.map(col => (
                      <MenuItem key={col.column_name} value={col.column_name}>{col.column_name}</MenuItem>
                    ))}
                  </Select>
                </FormControl>

                {/* Selector de Operador DINÁMICO */}
                <FormControl sx={{ minWidth: 180 }}>
                  <InputLabel>Operador</InputLabel>
                  <Select value={filter.operator} onChange={(e) => handleFilterChange(filter.id, 'operator', e.target.value)}>
                    {availableOperators.map(op => (
                      <MenuItem key={op.value} value={op.value}>{op.label}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
                
                {/* Campo(s) de Valor DINÁMICO */}
                {filter.operator === 'between' ? (
                  // Si el operador es 'between', mostramos DOS campos
                  <>
                    {dataTypeCategory === 'date' ? (
                      <>
                        <DatePicker
                          label="Fecha inicial"
                          value={filter.value[0] ? dayjs(filter.value[0]) : null}
                          onChange={(newValue) => handleBetweenValueChange(filter.id, 0, newValue ? newValue.format('YYYY-MM-DD') : '')}
                          renderInput={(params) => <TextField {...params} />}
                        />
                        <Typography>Y</Typography>
                        <DatePicker
                          label="Fecha final"
                          value={filter.value[1] ? dayjs(filter.value[1]) : null}
                          onChange={(newValue) => handleBetweenValueChange(filter.id, 1, newValue ? newValue.format('YYYY-MM-DD') : '')}
                          renderInput={(params) => <TextField {...params} />}
                        />
                      </>
                    ) : (
                      <>
                        <TextField
                          label="Valor inicial"
                          type={dataTypeCategory === 'numeric' ? 'number' : 'text'}
                          value={Array.isArray(filter.value) ? filter.value[0] || '' : ''}
                          onChange={(e) => handleBetweenValueChange(filter.id, 0, e.target.value)}
                        />
                        <Typography>Y</Typography>
                        <TextField
                          label="Valor final"
                          type={dataTypeCategory === 'numeric' ? 'number' : 'text'}
                          value={Array.isArray(filter.value) ? filter.value[1] || '' : ''}
                          onChange={(e) => handleBetweenValueChange(filter.id, 1, e.target.value)}
                        />
                      </>
                    )}
                  </>
                ) : (
                  // Si es cualquier otro operador, mostramos UN campo
                  (() => {
                    if (dataTypeCategory === 'date') {
                      return (
                        <DatePicker
                          label="Valor"
                          value={filter.value ? dayjs(filter.value) : null}
                          onChange={(newValue) => handleFilterChange(filter.id, 'value', newValue ? newValue.format('YYYY-MM-DD') : '')}
                          renderInput={(params) => <TextField {...params} sx={{ minWidth: 200 }} />}
                        />
                      );
                    } else if (dataTypeCategory === 'numeric') {
                      return <TextField label="Valor" type="number" value={filter.value} onChange={(e) => handleFilterChange(filter.id, 'value', e.target.value)} sx={{ minWidth: 200 }} />;
                    } else {
                      return <TextField label="Valor" type="text" value={filter.value} onChange={(e) => handleFilterChange(filter.id, 'value', e.target.value)} sx={{ minWidth: 200 }} />;
                    }
                  })()
                )}
                
                <Button onClick={() => removeFilter(filter.id)} color="error">X</Button>
              </Box>
            );
          })}
          {/* ================================================================== */}
          {/* FIN DE LA SECCIÓN MODIFICADA                                       */}
          {/* ================================================================== */}

          <Button onClick={addFilter} disabled={!selectedTable}>+ Añadir Filtro</Button>
        </Box>

        {/* --- BOTONES DE ACCIÓN (Sin cambios) --- */}
        <Box sx={{ display: 'flex', gap: 2, marginBottom: 4 }}>
          <Button variant="contained" onClick={handlePreview} disabled={loading || !selectedTable}>
            {loading ? <CircularProgress size={24} /> : 'Mostrar Preview'}
          </Button>
          <Button variant="outlined" onClick={() => handleDownload('csv')} disabled={loading || !selectedTable}>Descargar CSV</Button>
          <Button variant="outlined" onClick={() => handleDownload('xlsx')} disabled={loading || !selectedTable}>Descargar XLSX</Button>
        </Box>

        {/* --- ALERTA Y TABLA (Sin cambios) --- */}
        {error && <Alert severity="error" sx={{ marginBottom: 2 }}>{error}</Alert>}
        
        {previewData.length > 0 && (
          <Typography variant="h6" sx={{ marginBottom: 1 }}>
            Mostrando {previewData.length} de {totalCount} registros encontrados.
          </Typography>
        )}

        {previewData.length > 0 && (
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>{Object.keys(previewData[0]).map(key => <TableCell key={key}><b>{key}</b></TableCell>)}</TableRow>
              </TableHead>
              <TableBody>
                {previewData.map((row, index) => (
                  <TableRow key={index}>
                    {/* Cambiamos a Object.entries para tener acceso al nombre de la columna (key) */}
                    {Object.entries(row).map(([key, value]) => {
                      // Verificamos si la columna actual es 'comentarios'
                      const isComentarios = key.toLowerCase() === 'comentarios';
                      
                      // Definimos los estilos que se aplicarán solo a la celda de comentarios
                      const cellStyles = isComentarios ? {
                        maxWidth: 300,        // Limita el ancho máximo de la celda
                        whiteSpace: 'nowrap', // Evita que el texto salte de línea
                        overflow: 'auto',     // Añade scroll si el contenido se desborda
                      } : {};

                      return (
                        <TableCell key={key} sx={cellStyles}>
                          {String(value)}
                        </TableCell>
                      );
                    })}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Container>
    </LocalizationProvider>
  );}

export default App;
