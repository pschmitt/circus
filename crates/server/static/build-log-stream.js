"use strict";

for (const output of document.querySelectorAll("[data-build-log-stream]")) {
  const status = output.parentElement.querySelector("[data-build-log-status]");
  const source = new EventSource(output.dataset.buildLogStream);
  const maxLines = 5000;
  let connectedOnce = false;

  source.onopen = () => {
    if (connectedOnce) {
      output.replaceChildren();
    }
    connectedOnce = true;
    status.textContent = "Connected · waiting for build output";
  };

  source.onmessage = (event) => {
    const line = document.createElement("span");
    line.textContent = `${event.data}\n`;
    output.append(line);

    if (output.childElementCount > maxLines) {
      output.firstElementChild.remove();
    }
    output.scrollTop = output.scrollHeight;
  };

  source.addEventListener("done", () => {
    status.textContent = "Build finished · loading the completed log";
    source.close();
    window.setTimeout(() => window.location.reload(), 1200);
  });

  source.onerror = () => {
    if (source.readyState !== EventSource.CLOSED) {
      status.textContent = "Reconnecting to build log…";
    }
  };
}
