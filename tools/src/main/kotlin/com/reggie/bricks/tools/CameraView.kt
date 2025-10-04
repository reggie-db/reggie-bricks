// src/main/kotlin/com/reggie/bricks/tools/CameraView.kt
package com.reggie.bricks.tools

import com.vaadin.flow.component.ClientCallable
import com.vaadin.flow.component.Html
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.notification.Notification
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.progressbar.ProgressBar
import com.vaadin.flow.router.Route

@Route("")
class CameraView : VerticalLayout() {

    private val defaultFps = 10

    private val video = Html(
        """
        <video id="localVideo" autoplay playsinline muted
               style="display:none; width:720px; height:540px; background-color:black;">
        </video>
        """.trimIndent()
    )

    private val capturePanel = Html(
        """
        <div id="capturePanel" style="display:none; width:360px;">
          <div style="font-family:system-ui, sans-serif; margin-bottom:8px;">
            <b>Captures</b> <span id="fpsLabel" style="opacity:0.7">(10 fps)</span>
          </div>
          <div style="border:1px solid #ddd; border-radius:8px; padding:8px;">
            <img id="captureImg" alt="capture preview" style="width:100%; display:block; background:#111;"/>
          </div>
        </div>
        """.trimIndent()
    )

    private val start = Button("Start camera") {
        // call the JS bootstrapper which will invoke callback posting
        UI.getCurrent().page.executeJs(
            "window._startCameraImpl && window._startCameraImpl($0);",
            defaultFps
        )
    }.apply {
        setId("startBtn")
        element.style.set("display", "none")
    }

    private val permBar = ProgressBar().apply {
        setId("permBar")
        isIndeterminate = true
        element.style.set("width", "200px")
        element.style.set("display", "none")
    }

    init {
        setSizeFull()
        isPadding = true
        isSpacing = true

        val row = HorizontalLayout().apply {
            setWidthFull()
            add(video, capturePanel)
            expand(video)
        }

        add(HorizontalLayout(start, permBar), row)
        addAttachListener { bootstrapAndMaybeStart(defaultFps) }
        addDetachListener { stopCamera() }
    }

    private fun bootstrapAndMaybeStart(fps: Int) {
        val serverVar = "\$server"
        //language=javascript
        UI.getCurrent().page.executeJs(
            """
            (function() {
              const fps = $0 || 10;
              const startBtn = document.getElementById('startBtn');
              const bar = document.getElementById('permBar');

              const showStart = (show) => { if (startBtn) startBtn.style.display = show ? 'inline-block' : 'none'; };
              const showBar = (show) => { if (bar) bar.style.display = show ? 'inline-block' : 'none'; };

              // Expose a server-callback binder tied to this view element
              // $1 is the server-side element for this view passed from executeJs
              window._bindServerCapture = function(el){
                window._sendCapture = function(dataUrl){
                  el.${serverVar}.onCapture(dataUrl);
                }
              };
              window._bindServerCapture($1);

              window._startCameraImpl = function(fpsArg) {
                const effFps = fpsArg || fps;
                const video = document.getElementById('localVideo');
                const panel = document.getElementById('capturePanel');
                const img = document.getElementById('captureImg');
                const fpsLabel = document.getElementById('fpsLabel');

                if (fpsLabel) fpsLabel.textContent = effFps + ' fps';
                showBar(true);

                try {
                  if (window._captureTimer) { clearInterval(window._captureTimer); window._captureTimer = null; }
                  if (window._vaadinLocalStream) {
                    window._vaadinLocalStream.getTracks().forEach(t => t.stop());
                    window._vaadinLocalStream = null;
                  }
                } catch (_) {}

                if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
                  console.error('Camera API not available');
                  showBar(false);
                  showStart(true);
                  return;
                }

                navigator.mediaDevices.getUserMedia({ video: { facingMode: 'user' }, audio: false })
                  .then(stream => {
                    video.srcObject = stream;
                    video.style.display = 'block';
                    panel.style.display = 'block';
                    video.play();
                    window._vaadinLocalStream = stream;
                    showStart(false);
                    showBar(false);

                    const canvas = document.createElement('canvas');
                    const ctx = canvas.getContext('2d');

                    const captureOnce = () => {
                      const vw = video.videoWidth || 720;
                      const vh = video.videoHeight || 540;

                      const panelWidth = panel.clientWidth || 360;
                      const scale = panelWidth / vw;
                      const cw = Math.round(vw * scale);
                      const ch = Math.round(vh * scale);
                      canvas.width = cw;
                      canvas.height = ch;

                      ctx.drawImage(video, 0, 0, cw, ch);

                      const dataUrl = canvas.toDataURL('image/jpeg', 0.8);
                      img.src = dataUrl;

                      if (window._sendCapture) {
                        window._sendCapture(dataUrl);
                      } else {
                        // fallback direct POST if binder not ready for any reason
                        fetch('/api/capture', {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({ imageBase64: dataUrl })
                        });
                      }
                    };

                    const intervalMs = Math.max(1, Math.floor(1000 / effFps));
                    window._captureTimer = setInterval(captureOnce, intervalMs);
                  })
                  .catch(err => {
                    console.error('getUserMedia failed', err);
                    showBar(false);
                    showStart(true);
                  });
              };

              const autoRequest = () => {
                showStart(false);
                showBar(true);
                setTimeout(() => window._startCameraImpl(fps), 0);
              };

              if (navigator.permissions && navigator.permissions.query) {
                try {
                  navigator.permissions.query({ name: 'camera' }).then(p => {
                    if (p.state === 'granted') {
                      autoRequest();
                    } else if (p.state === 'prompt') {
                      autoRequest();
                    } else {
                      showStart(true);
                    }
                    p.onchange = () => {
                      if (p.state === 'granted') autoRequest();
                    };
                  }).catch(() => autoRequest());
                } catch (_) {
                  autoRequest();
                }
              } else {
                autoRequest();
              }
            })();
            """.trimIndent(),
            fps,
            element
        )
    }

    @ClientCallable
    fun onCapture(imageBase64: String) {
        // imageBase64 is a data URL like "data:image/jpeg;base64,/9j/4AAQSkZJRgABA..."
        // parse and handle as needed
        // for now just acknowledge on server side
        Notification.show("Capture received", 1500, Notification.Position.BOTTOM_START)
        println(imageBase64)
    }

    private fun stopCamera() {
        //language=javascript
        UI.getCurrent().page.executeJs(
            """
            try {
              const video = document.getElementById('localVideo');
              const panel = document.getElementById('capturePanel');
              const stopTracks = s => s && s.getTracks && s.getTracks().forEach(t => t.stop());

              if (window._captureTimer) {
                clearInterval(window._captureTimer);
                window._captureTimer = null;
              }
              if (video && video.srcObject) {
                stopTracks(video.srcObject);
                video.srcObject = null;
              }
              if (window._vaadinLocalStream) {
                stopTracks(window._vaadinLocalStream);
                window._vaadinLocalStream = null;
              }
              if (video) video.style.display = 'none';
              if (panel) panel.style.display = 'none';
            } catch (_) {}
            """.trimIndent()
        )
    }
}