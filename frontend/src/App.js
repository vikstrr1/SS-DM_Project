import React, { useEffect, useState } from 'react';
import Chart from './components/Chart';

const App = () => {
    const [data, setData] = useState(null);

    useEffect(() => {
        fetch('/api/data')
            .then(response => response.json())
            .then(data => setData(data));
    }, []);

    return (
        <div>
            <h1>Trading Trends Visualization</h1>
            {data && <Chart data={data} />}
        </div>
    );
};

export default App;
