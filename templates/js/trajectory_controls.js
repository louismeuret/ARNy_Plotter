var stage;
var player;
var trajComp; // To store the trajectory component
document.addEventListener("DOMContentLoaded", function () {
  stage = new NGL.Stage("viewport");
  var traj = "{{ url_for('static', filename='uploads/' + session_id + '/' + traj_xtc) }}";
  var load = "{{ url_for('static', filename='uploads/' + session_id + '/' + native_pdb) }}";
  if (load) {
    stage.loadFile(load, { defaultRepresentation: true }).then(function (o) {
      stage.setParameters({ backgroundColor: "white" });
      stage.setParameters({ fogFar: 1000 });
      stage.setParameters({ fogNear: 0 });
      stage.setParameters({ clipFar: 1000 });
      o.setName("simulation-name");

      NGL.autoLoad(traj).then(function (frames) {
        trajComp = o.addTrajectory(frames);
        trajComp.trajectory.setFrame(1);
        player = new NGL.TrajectoryPlayer(trajComp.trajectory, {
          interpolateType: "spline",
        });

        // Initialize slider value and max based on frames length
        var frameSlider = document.getElementById("frame-slider");
        frameSlider.max = trajComp.trajectory.frameCount - 1;
        frameSlider.value = 1;

        // Update frame when slider value changes
        frameSlider.addEventListener("input", updateFrame);
      });
    });
  }

  var toggleTheme = document.getElementById("toggleTheme");
  var isLight = false;
  toggleTheme.addEventListener("click", function () {
    stage.setParameters({ backgroundColor: isLight ? "black" : "white" });
    isLight = !isLight;
  });

  var toggleSpin = document.getElementById("toggleSpin");
  var isSpinning = false;
  toggleSpin.addEventListener("click", function () {
    stage.setSpin(isSpinning ? null : [0, 1, 0], isSpinning ? null : 0.01);
    isSpinning = !isSpinning;
  });

  var toggleSideChains = document.getElementById("toggleSideChains");
  toggleSideChains.addEventListener("click", function () {
    var representations = stage.getRepresentationsByName("licorice");
    if (representations) {
      representations.list.forEach(function (repre) {
        repre.setVisibility(!repre.visible);
      });
    }
  });

  var toggleRunMDs = document.getElementById("toggleRunMDs");
  var isRunning = false;
  toggleRunMDs.addEventListener("click", function () {
    if (trajComp) {
      if (!isRunning) {
        player.play();
      } else {
        player.pause();
      }
      isRunning = !isRunning;
    }
  });

  function updateFrame(event) {
    var frameIndex = parseInt(event.target.value);
    if (player) {
      trajComp.trajectory.setFrame(frameIndex);
      var frameSlider = document.getElementById("frame-slider");
      frameSlider.value = frameIndex;
    }
  }

  function togglePlayPause() {
    if (player) {
      player.toggle();
      var playPauseButton = document.getElementById("play-pause-button");
      playPauseButton.textContent = player.isPlaying ? "Pause" : "Play";
    }
  }

  // Attach event listener to play/pause button
  document
    .getElementById("play-pause-button")
    .addEventListener("click", togglePlayPause);
});
