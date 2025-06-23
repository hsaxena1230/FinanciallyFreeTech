// Add these functions to your JavaScript code

// Initialize index elements
function initIndexElements(elements) {
    elements.indexSection = document.getElementById('index-section');
    elements.indexSubtitle = document.getElementById('index-subtitle');
    elements.indexTypeSelect = document.getElementById('index-type-select');
    elements.indexNameSelect = document.getElementById('index-name-select');
    elements.indexCanvas = document.getElementById('index-canvas');
    elements.indexLoading = document.getElementById('index-loading');
    elements.indexError = document.getElementById('index-error');
    elements.indexCloseBtn = document.getElementById('index-close-btn');
    elements.showIndexBtn = document.getElementById('show-index-btn');
    
    // Add event listeners
    elements.indexTypeSelect.addEventListener('change', () => handleIndexTypeChange(elements));
    elements.indexNameSelect.addEventListener('change', () => handleIndexNameChange(elements));
    elements.indexCloseBtn.addEventListener('click', () => hideIndexSection(elements));
    elements.showIndexBtn.addEventListener('click', () => showIndexSection(elements));
}

// Show index section
function showIndexSection(elements) {
    elements.indexSection.style.display = 'block';
    elements.indexSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    
    // Fetch index types if not already fetched
    if (elements.indexTypeSelect.options.length <= 1) {
        fetchIndexTypes(elements);
    }
}

// Hide index section
function hideIndexSection(elements) {
    elements.indexSection.style.display = 'none';
    
    // Destroy existing chart if any
    if (state.indexChart) {
        state.indexChart.destroy();
        state.indexChart = null;
    }
}

// Fetch index types
async function fetchIndexTypes(elements) {
    try {
        elements.indexLoading.style.display = 'flex';
        elements.indexError.style.display = 'none';
        
        const response = await fetch(`${API_BASE_URL}/indices/types`);
        const data = await response.json();
        
        if (data.success) {
            // Add types to the select
            elements.indexTypeSelect.innerHTML = '<option value="">Select Type</option>';
            
            data.data.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = formatIndexType(type);
                elements.indexTypeSelect.appendChild(option);
            });
        } else {
            elements.indexError.textContent = data.error || 'Failed to fetch index types';
            elements.indexError.style.display = 'block';
        }
    } catch (err) {
        console.error('Error fetching index types:', err);
        elements.indexError.textContent = 'Error connecting to the server';
        elements.indexError.style.display = 'block';
    } finally {
        elements.indexLoading.style.display = 'none';
    }
}

// Handle index type change
async function handleIndexTypeChange(elements) {
    const indexType = elements.indexTypeSelect.value;
    
    // Reset index name select
    elements.indexNameSelect.innerHTML = '<option value="">Select Index</option>';
    elements.indexNameSelect.disabled = !indexType;
    
    // Clear any existing chart
    if (state.indexChart) {
        state.indexChart.destroy();
        state.indexChart = null;
    }
    
    if (!indexType) return;
    
    // Fetch index names for this type
    try {
        elements.indexLoading.style.display = 'flex';
        elements.indexError.style.display = 'none';
        
        const response = await fetch(`${API_BASE_URL}/indices/names?type=${encodeURIComponent(indexType)}`);
        const data = await response.json();
        
        if (data.success && data.data.length > 0) {
            // Add names to the select
            data.data.forEach(index => {
                const option = document.createElement('option');
                option.value = index.index_name;
                option.textContent = formatIndexName(index.index_name);
                option.dataset.count = index.constituent_count;
                elements.indexNameSelect.appendChild(option);
            });
            
            elements.indexNameSelect.disabled = false;
        } else {
            elements.indexError.textContent = data.error || 'No indices found for this type';
            elements.indexError.style.display = 'block';
            elements.indexNameSelect.disabled = true;
        }
    } catch (err) {
        console.error('Error fetching index names:', err);
        elements.indexError.textContent = 'Error connecting to the server';
        elements.indexError.style.display = 'block';
    } finally {
        elements.indexLoading.style.display = 'none';
    }
}

// Handle index name change
async function handleIndexNameChange(elements) {
    const indexName = elements.indexNameSelect.value;
    
    if (!indexName) {
        // Clear any existing chart
        if (state.indexChart) {
            state.indexChart.destroy();
            state.indexChart = null;
        }
        return;
    }
    
    // Get the selected option to access its dataset
    const selectedOption = elements.indexNameSelect.options[elements.indexNameSelect.selectedIndex];
    const constituentCount = selectedOption.dataset.count;
    
    // Update subtitle
    elements.indexSubtitle.textContent = `${formatIndexName(indexName)} - ${constituentCount} constituents`;
    
    // Fetch index data
    await fetchIndexData(indexName, elements);
}

// Fetch index data
async function fetchIndexData(indexName, elements) {
    try {
        elements.indexLoading.style.display = 'flex';
        elements.indexError.style.display = 'none';
        
        // Clear any existing chart
        if (state.indexChart) {
            state.indexChart.destroy();
            state.indexChart = null;
        }
        
        const response = await fetch(`${API_BASE_URL}/indices/data?name=${encodeURIComponent(indexName)}`);
        const data = await response.json();
        
        if (data.success && data.data.length > 0) {
            // Render the chart
            renderIndexChart(data.data, elements);
        } else {
            elements.indexError.textContent = data.error || 'No data found for this index';
            elements.indexError.style.display = 'block';
        }
    } catch (err) {
        console.error('Error fetching index data:', err);
        elements.indexError.textContent = 'Error connecting to the server';
        elements.indexError.style.display = 'block';
    } finally {
        elements.indexLoading.style.display = 'none';
    }
}

// Render index chart
function renderIndexChart(indexData, elements) {
    // Sort data by time
    indexData.sort((a, b) => new Date(a.time) - new Date(b.time));
    
    // Prepare data for the chart
    const times = indexData.map(item => new Date(item.time));
    const values = indexData.map(item => item.index_value);
    
    // Create the chart
    const ctx = elements.indexCanvas.getContext('2d');
    state.indexChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: times,
            datasets: [{
                label: 'Index Value',
                data: values,
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.1)',
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: '#f59e0b',
                pointHoverBorderColor: '#ffffff',
                pointHoverBorderWidth: 2,
                tension: 0.3,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function(tooltipItems) {
                            return new Date(tooltipItems[0].label).toLocaleDateString();
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'month',
                        displayFormats: {
                            month: 'MMM yyyy'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Date'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Index Value'
                    },
                    beginAtZero: false
                }
            }
        }
    });
}

// Format index type for display
function formatIndexType(type) {
    switch (type) {
        case 'sector':
            return 'Sector';
        case 'industry':
            return 'Industry';
        case 'sector_industry':
            return 'Sector-Industry';
        default:
            return type;
    }
}

// Format index name for display
function formatIndexName(name) {
    // Remove prefix
    if (name.startsWith('SECTOR-')) {
        name = name.replace('SECTOR-', '');
    } else if (name.startsWith('INDUSTRY-')) {
        name = name.replace('INDUSTRY-', '');
    } else if (name.startsWith('SECTOR-INDUSTRY-')) {
        name = name.replace('SECTOR-INDUSTRY-', '');
    }
    
    // Replace hyphens with spaces
    name = name.replace(/-/g, ' ');
    
    return name;
}
