(function () {
  const errorClassName = "is-error";
  const granularityName = "granularity";
  const idleMessage = "Idle";
  const loadingClassName = "is-loading";
  const loadingMessage = "Loading data...";
  const nodeLimitAutoValue = "auto";
  const presetBehavior = "preset";
  const searchBehavior = "search";
  const statusErrorMessage = "Request failed.";
  const statusSelector = "#loading-indicator";

  let searchTimeoutId = null;

  function initialize(root) {
    const form = root.querySelector("#filters-form");
    if (form && !form.dataset.initialized) {
      form.dataset.initialized = "true";
      bindForm(form);
    }

    const histogram = root.querySelector("#histogram");
    if (histogram && !histogram.dataset.initialized) {
      histogram.dataset.initialized = "true";
      bindHistogram(histogram);
    }
  }

  function bindForm(form) {
    const presetSelect = form.querySelector(`[data-behavior="${presetBehavior}"]`);
    const searchInput = form.querySelector(`[data-behavior="${searchBehavior}"]`);
    const nodeLimitSelect = form.querySelector("#node-limit");

    form.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      if (target.matches(`[data-behavior="${presetBehavior}"]`)) {
        applyPreset(form, target.value);
        submitForm(form);
        return;
      }

      if (target.getAttribute("name") === granularityName && nodeLimitSelect) {
        nodeLimitSelect.value = nodeLimitAutoValue;
      }

      submitForm(form);
    });

    if (searchInput) {
      searchInput.addEventListener("input", () => {
        window.clearTimeout(searchTimeoutId);
        searchTimeoutId = window.setTimeout(() => submitForm(form), 250);
      });
    }

    if (presetSelect) {
      applyPreset(form, presetSelect.value);
    }
  }

  function bindHistogram(histogram) {
    const svg = histogram.querySelector("svg");
    const form = document.querySelector("#filters-form");
    const bars = Array.from(histogram.querySelectorAll(".histogram-bar"));
    if (!svg || !form || bars.length === 0) {
      return;
    }

    let dragStartIndex = null;
    let overlay = null;

    function indexFromClientX(clientX) {
      const bounds = svg.getBoundingClientRect();
      const relative = Math.max(0, Math.min(bounds.width, clientX - bounds.left));
      return Math.min(bars.length - 1, Math.floor((relative / bounds.width) * bars.length));
    }

    function clearOverlay() {
      dragStartIndex = null;
      if (overlay) {
        overlay.remove();
        overlay = null;
      }
    }

    svg.addEventListener("mousedown", (event) => {
      dragStartIndex = indexFromClientX(event.clientX);
      overlay = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      overlay.setAttribute("y", "0");
      overlay.setAttribute("height", "100%");
      overlay.setAttribute("fill", "rgba(88, 126, 163, 0.18)");
      svg.appendChild(overlay);
    });

    svg.addEventListener("mousemove", (event) => {
      if (dragStartIndex === null || !overlay) {
        return;
      }

      const currentIndex = indexFromClientX(event.clientX);
      const barWidth = svg.viewBox.baseVal.width / bars.length;
      const left = Math.min(dragStartIndex, currentIndex) * barWidth;
      const right = (Math.max(dragStartIndex, currentIndex) + 1) * barWidth;
      overlay.setAttribute("x", `${left}`);
      overlay.setAttribute("width", `${Math.max(3, right - left)}`);
    });

    svg.addEventListener("mouseleave", clearOverlay);

    svg.addEventListener("mouseup", (event) => {
      if (dragStartIndex === null) {
        return;
      }

      const currentIndex = indexFromClientX(event.clientX);
      const startIndex = Math.min(dragStartIndex, currentIndex);
      const endIndex = Math.max(dragStartIndex, currentIndex);
      setRange(form, bars[startIndex].dataset.fromNs, bars[endIndex].dataset.toNs);
      clearOverlay();
      submitForm(form);
    });

    svg.addEventListener("dblclick", () => {
      resetRange(form);
      submitForm(form);
    });
  }

  function applyPreset(form, presetValue) {
    const appShell = document.querySelector("#app-shell");
    if (!appShell) {
      return;
    }

    const spanStartNs = Number.parseInt(appShell.dataset.spanStartNs || "0", 10);
    const spanEndNs = Number.parseInt(appShell.dataset.spanEndNs || "0", 10);
    if (!spanStartNs || !spanEndNs) {
      return;
    }

    let fromNs = spanStartNs;
    let toNs = spanEndNs;

    switch (presetValue) {
      case "1h":
        fromNs = Math.max(spanStartNs, spanEndNs - 3600e9);
        break;
      case "24h":
        fromNs = Math.max(spanStartNs, spanEndNs - 24 * 3600e9);
        break;
      case "7d":
        fromNs = Math.max(spanStartNs, spanEndNs - 7 * 24 * 3600e9);
        break;
      case "30d":
        fromNs = Math.max(spanStartNs, spanEndNs - 30 * 24 * 3600e9);
        break;
      default:
        break;
    }

    setRange(form, String(fromNs), String(toNs));
  }

  function setRange(form, fromNs, toNs) {
    const fromInput = form.querySelector("#filter-from-ns");
    const toInput = form.querySelector("#filter-to-ns");
    if (fromInput) {
      fromInput.value = fromNs;
    }
    if (toInput) {
      toInput.value = toNs;
    }
  }

  function resetRange(form) {
    const appShell = document.querySelector("#app-shell");
    if (!appShell) {
      return;
    }

    setRange(form, appShell.dataset.spanStartNs || "", appShell.dataset.spanEndNs || "");

    const presetSelect = form.querySelector(`[data-behavior="${presetBehavior}"]`);
    if (presetSelect) {
      presetSelect.value = "all";
    }
  }

  function submitForm(form) {
    form.requestSubmit();
  }

  function statusElement() {
    return document.querySelector(statusSelector);
  }

  function setLoadingStatus(message) {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = message;
    element.classList.add(loadingClassName);
    element.classList.remove(errorClassName);
  }

  function setErrorStatus(message) {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = message;
    element.classList.add(errorClassName);
    element.classList.remove(loadingClassName);
  }

  function clearStatus() {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = idleMessage;
    element.classList.remove(loadingClassName, errorClassName);
  }

  document.addEventListener("DOMContentLoaded", () => initialize(document));
  document.body.addEventListener("htmx:beforeRequest", () => setLoadingStatus(loadingMessage));
  document.body.addEventListener("htmx:afterSwap", (event) => {
    if (event.target instanceof HTMLElement) {
      initialize(event.target);
    }
    clearStatus();
  });
  document.body.addEventListener("htmx:responseError", (event) => {
    const detail = event.detail;
    if (detail && detail.xhr) {
      setErrorStatus(detail.xhr.responseText || statusErrorMessage);
      return;
    }
    setErrorStatus(statusErrorMessage);
  });
})();
