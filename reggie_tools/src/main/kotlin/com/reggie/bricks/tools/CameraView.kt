package com.reggie.bricks.tools


import com.vaadin.flow.component.Html
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.router.Route

@Route("")
class CameraView : VerticalLayout() {

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

    private val start = Button("Start camera") { startCamera(5) }
    private val stop = Button("Stop") { stopCamera() }

    init {
        setSizeFull()
        isPadding = true
        isSpacing = true

        val row = HorizontalLayout().apply {
            setWidthFull()
            add(video, capturePanel)
            expand(video)
        }

        add(HorizontalLayout(start, stop), row)
        addDetachListener { stopCamera() }
    }

    private fun startCamera(fps: Int = 10) {
        UI.getCurrent().page.executeJs(
            // language=JavaScript
            """
            const fps = $0 || 10;
            const video = document.getElementById('localVideo');
            const panel = document.getElementById('capturePanel');
            const img = document.getElementById('captureImg');
            const fpsLabel = document.getElementById('fpsLabel');

            fpsLabel.textContent = `(${fps} fps)`;

            if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
              alert('Camera API not available in this browser');
              return;
            }

            // Stop any previous session first
            if (window._vaadinLocalStream) {
              try { window._vaadinLocalStream.getTracks().forEach(t => t.stop()); } catch {}
              window._vaadinLocalStream = null;
            }
            if (window._captureTimer) {
              clearInterval(window._captureTimer);
              window._captureTimer = null;
            }

            navigator.mediaDevices.getUserMedia({ video: { facingMode: 'user' }, audio: false })
              .then(stream => {
                video.srcObject = stream;
                video.style.display = 'block';
                panel.style.display = 'block';
                video.play();
                window._vaadinLocalStream = stream;

                // Prepare a reusable canvas for captures
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');

                const captureOnce = () => {
                  const vw = video.videoWidth || 720;
                  const vh = video.videoHeight || 540;

                  // Scale capture to panel width while keeping aspect
                  const panelWidth = panel.clientWidth || 360;
                  const scale = panelWidth / vw;
                  const cw = Math.round(vw * scale);
                  const ch = Math.round(vh * scale);
                  canvas.width = cw;
                  canvas.height = ch;

                  ctx.drawImage(video, 0, 0, cw, ch);

                  // Convert to JPEG for later posting
                  const dataUrl = canvas.toDataURL('image/jpeg', 0.8);
                  img.src = dataUrl;

                  // Example post stub for later
                  // fetch('/upload', { method: 'POST', body: dataUrl })
                };

                // Start capture loop at requested fps
                const intervalMs = Math.max(1, Math.floor(1000 / fps));
                window._captureTimer = setInterval(captureOnce, intervalMs);
              })
              .catch(err => {
                console.error('getUserMedia failed', err);
                alert('Unable to access camera');
              });
            """.trimIndent(),
            fps
        )
    }

    private fun stopCamera() {
        UI.getCurrent().page.executeJs(
            // language=JavaScript
            """
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
            """.trimIndent()
        )
    }
}