# 📊 Aster Dex Liquidation Dashboard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)

A real-time dashboard for monitoring liquidations on the Aster Dex exchange. This application provides live updates on liquidation events, including volume, symbol distribution, and historical data visualization.

## 🌟 Features

- Real-time WebSocket connection to Aster Dex
- Live liquidation event tracking
- Interactive charts and tables
- Responsive design for all devices
- Historical data visualization
- Key metrics and statistics

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/aster-lik-dash.git
   cd aster-lik-dash
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Application

```bash
streamlit run streamlit_app.py
```

Then open your browser and navigate to `http://localhost:8501`.

## 🛠️ Project Structure

```
aster-lik-dash/
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
├── streamlit_app.py
└── static/
    └── logo.svg
```

## 📊 Data Visualization

The dashboard includes several visualizations:

- **Real-time Liquidation Feed**: Shows the latest liquidation events
- **Liquidation Volume by Symbol**: Bar chart of liquidation volumes
- **Key Metrics**: Total liquidations, 24h volume, top symbols, and more

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Streamlit](https://streamlit.io/)
- Data provided by [Aster Dex](https://www.asterdex.com/)
- Icons from [Font Awesome](https://fontawesome.com/)

## 📧 Contact

Cicero Jacobi - [@yourtwitter](https://twitter.com/yourtwitter)

Project Link: [https://github.com/yourusername/aster-lik-dash](https://github.com/yourusername/aster-lik-dash)
