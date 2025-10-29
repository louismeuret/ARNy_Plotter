
# ARNy Plotter — Trajectory Plotting Web Application for RNA

**ARNy Plotter** is a web-based visualization and analysis tool designed for exploring RNA simulation trajectories. It supports a wide range of molecular dynamics (MD) formats, and provides publication-ready plots including contact maps, RMSD, eRMSD, torsions, dot-bracket transitions, and more.

---

## 🛠️ Installation

To install the program locally, follow these steps:

### 1. Create a Conda environment

```bash
conda create --name ARNy_Plotter python=3.13.2
conda activate ARNy_Plotter
````

### 2. Run the installation script

```bash
python <(curl -s https://raw.githubusercontent.com/louismeuret/Plot_RNA2/refs/heads/main/install_script.py) --repo https://github.com/louismeuret/Plot_RNA2 --install-path .
```

### 3. Launch the server

For the moment, only the linux version has been tested.

```bash
./run_program.sh
```

After, just type in your browser http://127.0.0.1:4242
You sould (hopefully) be able to access the webserver.

---

## 🧪 How to Use the Web Interface

### Step-by-step Usage:

1. **Upload Your Input Files**

   * A **topology** file (also acts as a native structure file).
   * A **trajectory** file (`.xtc`, `.trr`, `.dcd`, etc.).

2. **Configure Trajectory Processing** *(Optional)*

   * Set **stride** to skip frames.
   * Choose **start/end frames** to limit the analysis range.

3. **Select Desired Plots**
   Choose among Contact Map, RMSD, eRMSD, Torsions, Dot-Bracket, Arc, Secondary Structure, etc.

4. **Start Calculation**
   Click **Upload** to launch computations. Processing time depends on file size and selected plots.

5. **Session Handling**

   * Use "Share session" to create a permanent link to your results.
   * Use "Retrieve previous results" with a session ID to re-access them later.

---

## Plot Documentation

### 1. Contact Map Plot

* Displays contact pairs over time using Barnaba annotations.
* Interactive: changes with viewer's current frame.
* Built with **Plotly**.

### 2. RMSD Plot

* Root Mean Square Deviation over time.
* Compared to the native structure using all atoms.
* Computed via **Barnaba**.

### 3. eRMSD Plot

* Enhanced RMSD based on base-pair geometries.
* Based on Bottaro et al., 2014.
* More sensitive to RNA structure-specific deviations.

### 4. Torsion Angles Plot

* Plot specific torsions like Eta vs Theta.
* Requires chain/position input (`CHAIN_POSITION_`).

### 5. Secondary Structure Plot

* Generated with Barnaba.
* Visualized using **Forna**.

### 6. Arc Plot

* Visualizes pairing frequencies via colored arcs.
* Shows base-pair persistence over time.

### 7. Dot-Bracket Plot

* Plots dot-bracket structure transitions across frames.
* Great for visualizing secondary structure changes over time.

### 8. 2D Base Pairing *(WIP)*

* Still in development, intended to show 2D representations of pairings.

---

## ⚠️ Known Issues & Limitations

* Stride controls may cause instability.
* Viewer controls under Mol\* trajectory player are partially broken.
* Mol\* viewer can't handle trajectories >2GB (Cloudflare tunnel also limits upload size).
* Not all file formats tested – relies on **MDAnalysis**/**MDTraj** support.
* Plot-generated files download is currently **partially-functional**.
* Page reloads **recompute data** — use session sharing to avoid this.
* Large data can crash the interface — use stride or frame ranges to reduce load.

---

## 🖥️ UI Overview

* **Top Bar**: Server controls and links.
* **Session ID**: Unique key to retrieve results.
* **Share Session**: Generates a permanent link to your results.
* **Viewer Pane**: Visualize trajectory with Mol\* viewer (frame selection, sidechain toggles, etc.).
* **Plots Plane**: Scrollable view of all generated plots.
* **Download Options**: When available, you can download plot data (CSV) or images (HTML).

---

## 📫 Contact

For questions, issues, or suggestions, please open an issue on the [GitHub repository](https://github.com/louismeuret/Plot_RNA2), or send me an email at louis.meuret [at] etu.u-paris.fr

---

## License

[MIT License](LICENSE)


