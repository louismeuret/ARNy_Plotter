// app.js

let letters = ['A', 'U', 'G', 'C'];
let colors = {
  'A': '#FF0000', // Red
  'U': '#00FF00', // Green
  'G': '#0000FF', // Blue
  'C': '#FFFF00'  // Yellow
};
let grid = [];
let cols, rows;
let gridSize = 15; // Size of each cell in the grid
let highlightRadius = 100; // Radius of the "flashlight"
let highlightRadiusSquared = highlightRadius * highlightRadius;

function setup() {
  createCanvas(windowWidth, document.body.scrollHeight);
  cols = floor(width / gridSize);
  rows = floor(height / gridSize);
  
  // Initialize grid with random letters
  for (let i = 0; i < cols; i++) {
    grid[i] = [];
    for (let j = 0; j < rows; j++) {
      let letter = random(letters);
      grid[i][j] = letter;
    }
  }
  
  drawGrid();
}

function draw() {
  clear(); // Clear only the necessary part of the canvas
  
  for (let i = 0; i < cols; i++) {
    for (let j = 0; j < rows; j++) {
      let x = i * gridSize;
      let y = j * gridSize;
      let letter = grid[i][j];
      
      // Check squared distance to the cursor for performance
      let dx = mouseX - (x + gridSize / 2);
      let dy = mouseY - (y + gridSize / 2);
      let distanceSquared = dx * dx + dy * dy;
      
      if (distanceSquared < highlightRadiusSquared) {
        // Closer to the cursor, more visible
        fill(color(colors[letter]));
      } else {
        // Further from the cursor, more faint
        fill(lerpColor(color(colors[letter]), color(255), 0.8)); // Adjust the 0.8 to control the faintness
      }
      
      textAlign(CENTER, CENTER);
      textSize(gridSize * 0.8);
      text(letter, x + gridSize / 2, y + gridSize / 2);
    }
  }
}

function drawGrid() {
  background(255); // White background

  for (let i = 0; i < cols; i++) {
    for (let j = 0; j < rows; j++) {
      let x = i * gridSize;
      let y = j * gridSize;
      let letter = grid[i][j];
      
      fill(lerpColor(color(colors[letter]), color(255), 0.8)); // Faint color
      textAlign(CENTER, CENTER);
      textSize(gridSize * 0.8);
      text(letter, x + gridSize / 2, y + gridSize / 2);
    }
  }
}

function windowResized() {
  resizeCanvas(windowWidth, windowHeight);
  cols = floor(width / gridSize);
  rows = floor(height / gridSize);
  
  // Reinitialize grid with random letters
  grid = [];
  for (let i = 0; i < cols; i++) {
    grid[i] = [];
    for (let j = 0; j < rows; j++) {
      let letter = random(letters);
      grid[i][j] = letter;
    }
  }
  
  drawGrid();
}

