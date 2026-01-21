// app.js

// Aliases for Matter.js
const Engine = Matter.Engine,
      World = Matter.World,
      Bodies = Matter.Bodies,
      Body = Matter.Body,
      Vector = Matter.Vector,
      Constraint = Matter.Constraint;

let engine, world;

const atomTypes = {
  H: { color: '#FFB3BA', radius: 3, bonds: 1 },  // Pastel pink
  C: { color: '#C2C2A3', radius: 6, bonds: 4 },  // Pastel grey
  N: { color: '#B4E1FF', radius: 5, bonds: 3 },  // Pastel blue
  O: { color: '#FFDFBA', radius: 4, bonds: 2 },  // Pastel orange
  F: { color: '#BAE1FF', radius: 3, bonds: 1 }   // Pastel light blue
};

let atoms = [];
let bonds = [];
let cursorRepulsionStrength = 0.001; // Weak repulsive force from cursor

function setup() {
  createCanvas(windowWidth, document.body.scrollHeight);
  engine = Engine.create();
  world = engine.world;
  world.gravity.y = 0; // Disable gravity for a 2D space

  // Distribution percentages
  let numAtoms = 200;
  let numH = Math.floor(numAtoms * 0.50);
  let numC = Math.floor(numAtoms * 0.20);
  let numN = Math.floor(numAtoms * 0.10);
  let numO = Math.floor(numAtoms * 0.10);
  let numF = numAtoms - (numH + numC + numN + numO);

  // Create atoms based on distribution
  createAtoms('H', numH);
  createAtoms('C', numC);
  createAtoms('N', numN);
  createAtoms('O', numO);
  createAtoms('F', numF);

  Engine.run(engine);
}

function draw() {
  background(255); // White background

  // Update and display atoms
  atoms.forEach(atom => {
    moveAtom(atom);
    displayAtom(atom);
  });

  // Check for bonds
  bonds = [];
  for (let i = 0; i < atoms.length; i++) {
    for (let j = i + 1; j < atoms.length; j++) {
      if (canBond(atoms[i], atoms[j])) {
        let bond = createBond(atoms[i], atoms[j]);
        bonds.push(bond);
      }
    }
  }

  // Display bonds
  bonds.forEach(bond => {
    displayBond(bond);
  });
}

function createAtoms(type, count) {
  for (let i = 0; i < count; i++) {
    let atom = createAtom(type, random(width), random(height));
    atoms.push(atom);
  }
}

function createAtom(type, x, y) {
  let properties = atomTypes[type];
  let body = Bodies.circle(x, y, properties.radius, { restitution: 0.9 });
  body.atomType = type;
  body.bonds = [];
  World.add(world, body);
  return body;
}

function displayAtom(atom) {
  let properties = atomTypes[atom.atomType];
  fill(properties.color);
  noStroke();
  ellipse(atom.position.x, atom.position.y, properties.radius * 2);
}

function canBond(atom1, atom2) {
  let dist = Vector.magnitude(Vector.sub(atom1.position, atom2.position));
  let combinedRadius = atomTypes[atom1.atomType].radius + atomTypes[atom2.atomType].radius;

  // Check distance and bonding capacity
  if (dist < combinedRadius * 2 && atom1.bonds.length < atomTypes[atom1.atomType].bonds && atom2.bonds.length < atomTypes[atom2.atomType].bonds) {
    return true;
  }
  return false;
}

function createBond(atom1, atom2) {
  let bond = Constraint.create({
    bodyA: atom1,
    bodyB: atom2,
    length: Vector.magnitude(Vector.sub(atom1.position, atom2.position)),
    stiffness: 0.1
  });
  atom1.bonds.push(atom2);
  atom2.bonds.push(atom1);
  World.add(world, bond);
  return bond;
}

function displayBond(bond) {
  stroke(200); // Light grey for bonds
  line(bond.bodyA.position.x, bond.bodyA.position.y, bond.bodyB.position.x, bond.bodyB.position.y);
}

function moveAtom(atom) {
  let randomForce = Vector.create((Math.random() - 0.5) * 0.0001, (Math.random() - 0.5) * 0.0001);
  Body.applyForce(atom, atom.position, randomForce);

  // Repulsion from other atoms
  atoms.forEach(otherAtom => {
    if (atom !== otherAtom) {
      let dir = Vector.sub(atom.position, otherAtom.position);
      let distance = Vector.magnitude(dir);
      if (distance < 50) {
        let forceMagnitude = 0.002 / (distance * distance);
        let repulsionForce = Vector.mult(Vector.normalise(dir), forceMagnitude);
        Body.applyForce(atom, atom.position, repulsionForce);
      }
    }
  });

  // Repulsion from cursor
  let cursorDir = Vector.sub(atom.position, Vector.create(mouseX, mouseY));
  let cursorDistance = Vector.magnitude(cursorDir);
  if (cursorDistance < 100) {
    let cursorForce = Vector.mult(Vector.normalise(cursorDir), cursorRepulsionStrength);
    Body.applyForce(atom, atom.position, cursorForce);
  }

  // Contain within window
  if (atom.position.x < 0 || atom.position.x > width) {
    Body.setVelocity(atom, { x: -atom.velocity.x, y: atom.velocity.y });
    Body.setPosition(atom, { x: constrain(atom.position.x, 0, width), y: atom.position.y });
  }
  if (atom.position.y < 0 || atom.position.y > height) {
    Body.setVelocity(atom, { x: atom.velocity.x, y: -atom.velocity.y });
    Body.setPosition(atom, { x: atom.position.x, y: constrain(atom.position.y, 0, height) });
  }
}

function windowResized() {
  resizeCanvas(windowWidth, windowHeight);
}

