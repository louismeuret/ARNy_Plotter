/**
 * Plot Utilities - Shared functions for all Plotly-based plots
 * This file consolidates common functionality to reduce code duplication
 * and improve performance across plot templates.
 */

(function(window) {
  'use strict';

  // Create namespace for plot utilities
  var PlotUtils = window.PlotUtils = window.PlotUtils || {};

  /**
   * Debounce function - prevents excessive function calls
   * @param {Function} func - Function to debounce
   * @param {number} wait - Wait time in milliseconds
   * @returns {Function} Debounced function
   */
  PlotUtils.debounce = function(func, wait) {
    var timeout;
    return function() {
      var context = this;
      var args = arguments;
      clearTimeout(timeout);
      timeout = setTimeout(function() {
        func.apply(context, args);
      }, wait);
    };
  };

  /**
   * Throttle function - limits function calls to once per interval
   * @param {Function} func - Function to throttle
   * @param {number} limit - Minimum time between calls in milliseconds
   * @returns {Function} Throttled function
   */
  PlotUtils.throttle = function(func, limit) {
    var inThrottle;
    return function() {
      var context = this;
      var args = arguments;
      if (!inThrottle) {
        func.apply(context, args);
        inThrottle = true;
        setTimeout(function() {
          inThrottle = false;
        }, limit);
      }
    };
  };

  /**
   * Socket connection manager - maintains a single socket connection per session
   */
  var socketCache = {};
  PlotUtils.getSocket = function(sessionId) {
    if (!socketCache[sessionId]) {
      var protocol = location.protocol === 'https:' ? 'https://' : 'http://';
      socketCache[sessionId] = io.connect(protocol + document.domain + ':' + location.port, {
        query: 'session_id=' + sessionId
      });
    }
    return socketCache[sessionId];
  };

  /**
   * Default Plotly configuration
   */
  PlotUtils.defaultConfig = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false
  };

  /**
   * Default Plotly layout with minimal margins
   */
  PlotUtils.defaultLayout = {
    margin: { t: 10, r: 10, l: 50, b: 50 },
    autosize: true
  };

  /**
   * Initialize a Plotly plot with common settings
   * @param {string} plotId - The plot container element ID
   * @param {Array|Object} data - Plotly data
   * @param {Object} layoutOverrides - Optional layout overrides
   * @param {Object} configOverrides - Optional config overrides
   * @returns {Promise} Plotly.newPlot promise
   */
  PlotUtils.initPlot = function(plotId, data, layoutOverrides, configOverrides) {
    var layout = Object.assign({}, PlotUtils.defaultLayout, layoutOverrides || {});
    var config = Object.assign({}, PlotUtils.defaultConfig, configOverrides || {});

    return Plotly.newPlot(plotId, data, layout, config).then(function() {
      // Force resize after a short delay to ensure container is ready
      setTimeout(function() {
        var el = document.getElementById(plotId);
        if (el) {
          Plotly.Plots.resize(el);
        }
      }, 100);
    });
  };

  /**
   * Resize handler registry - manages resize listeners to avoid duplicates
   */
  var resizeHandlers = {};
  var globalResizeHandler = null;

  PlotUtils.registerResize = function(plotId) {
    if (!resizeHandlers[plotId]) {
      resizeHandlers[plotId] = true;

      // Set up global resize handler once
      if (!globalResizeHandler) {
        globalResizeHandler = PlotUtils.debounce(function() {
          Object.keys(resizeHandlers).forEach(function(id) {
            var el = document.getElementById(id);
            if (el) {
              Plotly.Plots.resize(el);
            }
          });
        }, 250);
        window.addEventListener('resize', globalResizeHandler);
      }
    }
  };

  PlotUtils.unregisterResize = function(plotId) {
    delete resizeHandlers[plotId];
  };

  /**
   * Handle frame selection from a plot click
   * Updates 3D viewer, frame markers, slider, and contact map
   * @param {number} frameNumber - The frame number to navigate to
   * @param {string} sessionId - The session ID for socket communication
   */
  PlotUtils.handleFrameSelection = function(frameNumber, sessionId) {
    // Update 3D viewer
    if (typeof LouisMolStarWrapper !== 'undefined' && LouisMolStarWrapper.interactivity) {
      LouisMolStarWrapper.interactivity.changeFrame(frameNumber);
    }

    // Update slider and frame number input
    var frameSlider = document.getElementById('frame-slider');
    var frameNumberInput = document.getElementById('frame-number');
    if (frameSlider) frameSlider.value = frameNumber;
    if (frameNumberInput) frameNumberInput.value = frameNumber;

    // Update all frame markers (debounced)
    if (typeof frameMarkers !== 'undefined') {
      if (frameMarkers.updateAllDebounced) {
        frameMarkers.updateAllDebounced(frameNumber);
      } else {
        frameMarkers.updateAll(frameNumber);
      }
    }

    // Update contact map via socket
    if (sessionId && typeof io !== 'undefined') {
      var socket = PlotUtils.getSocket(sessionId);
      socket.emit('update_contact_map', {
        value: frameNumber,
        session_id: sessionId
      }, sessionId);
    }
  };

  /**
   * Set up click handler for time-series plots
   * @param {string} plotId - The plot element ID
   * @param {string} sessionId - The session ID
   * @param {Function} frameExtractor - Optional function to extract frame from click data
   */
  PlotUtils.setupClickHandler = function(plotId, sessionId, frameExtractor) {
    var plotEl = document.getElementById(plotId);
    if (!plotEl) return;

    plotEl.on('plotly_click', function(data) {
      var frameNumber;
      if (frameExtractor) {
        frameNumber = frameExtractor(data);
      } else {
        // Default: assume x-axis is frame number
        frameNumber = data.points[0].x;
      }

      if (frameNumber !== undefined && frameNumber !== null) {
        PlotUtils.handleFrameSelection(frameNumber, sessionId);
      }
    });
  };

  /**
   * Set up expand icon click handler for a specific plot
   * @param {HTMLElement} container - The plot container element
   * @param {string} plotId - The plot element ID
   */
  PlotUtils.setupExpandHandler = function(container, plotId) {
    var expandIcon = container.querySelector('.expand-icon');
    if (expandIcon) {
      expandIcon.addEventListener('click', function() {
        setTimeout(function() {
          var el = document.getElementById(plotId);
          if (el) {
            Plotly.Plots.resize(el);
          }
        }, 100);
      });
    }
  };

  /**
   * Deferred registration queue for frame markers
   */
  var deferredRegistrations = [];

  /**
   * Register plot with frame markers if available, or queue for later
   * @param {string} plotId - The plot element ID
   * @param {Array|Object} plotData - The plot data
   */
  PlotUtils.registerFrameMarkers = function(plotId, plotData) {
    if (typeof frameMarkers !== 'undefined') {
      frameMarkers.register(plotId, plotData);
      console.log(`Registered plot ${plotId} with frameMarkers`);
    } else {
      // Queue registration for when frameMarkers becomes available
      deferredRegistrations.push({plotId: plotId, plotData: plotData});
      console.log(`Queued registration for plot ${plotId}`);
    }
  };

  /**
   * Process any deferred registrations when frameMarkers becomes available
   */
  PlotUtils.processDeferredRegistrations = function() {
    if (typeof frameMarkers !== 'undefined' && deferredRegistrations.length > 0) {
      console.log(`Processing ${deferredRegistrations.length} deferred frame marker registrations`);
      deferredRegistrations.forEach(function(reg) {
        frameMarkers.register(reg.plotId, reg.plotData);
        console.log(`Processed deferred registration for plot ${reg.plotId}`);
      });
      deferredRegistrations = []; // Clear the queue
    }
  };

  /**
   * Create a mode toggle handler for dual-mode plots
   * @param {string} plotId - The plot element ID
   * @param {Object} modes - Object mapping mode names to {data, layout, buttonId}
   * @param {string} initialMode - Initial mode name
   * @returns {Function} Mode switch function
   */
  PlotUtils.createModeToggle = function(plotId, modes, initialMode) {
    var currentMode = initialMode;

    return function(mode) {
      var modeConfig = modes[mode];
      if (!modeConfig) return;

      var plotDiv = document.getElementById(plotId);
      if (!plotDiv) return;

      // Update plot
      Plotly.react(plotDiv, modeConfig.data, modeConfig.layout || PlotUtils.defaultLayout);

      // Update button states
      Object.keys(modes).forEach(function(m) {
        var btn = document.getElementById(modes[m].buttonId);
        if (btn) {
          if (m === mode) {
            btn.classList.add('is-active');
          } else {
            btn.classList.remove('is-active');
          }
        }
      });

      currentMode = mode;

      // Force resize
      setTimeout(function() {
        Plotly.Plots.resize(plotDiv);
      }, 100);

      // Re-register with frame markers if applicable
      if (modeConfig.data) {
        PlotUtils.registerFrameMarkers(plotId, modeConfig.data);
      }

      return currentMode;
    };
  };

  /**
   * Get current mode for a toggle plot
   * @param {string} plotId - The plot element ID
   * @returns {string} Current mode name
   */
  PlotUtils.getCurrentMode = function(plotId) {
    var el = document.getElementById(plotId);
    return el ? el.dataset.currentMode : null;
  };

})(window);
