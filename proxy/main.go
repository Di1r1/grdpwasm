package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func noCacheHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		h.ServeHTTP(w, r)
	})
}

// gzipHandler сжимает статику на лету (WASM ~10МБ → ~3МБ). WebSocket /ws
// не проходит через него (обрабатывается отдельным handler'ом).
func gzipHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		h.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	Writer *gzip.Writer
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	return g.Writer.Write(b)
}

// WriteHeader: снимает Content-Length (он неверен после сжатия), пишет код.
func (g *gzipResponseWriter) WriteHeader(code int) {
	g.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipResponseWriter) Flush() {
	_ = g.Writer.Flush()
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

var allowedTargets []string

func normalizeTarget(s string) string {
	// автозамена на телефонах часто подставляет пробелы вместо точек в IP
	// (192 168.3.201). Убираем все пробелы и запятые-опечатки.
	s = strings.Map(func(r rune) rune {
		if r == ' ' || r == ',' || r == ';' || r == '\t' {
			return '.'
		}
		return r
	}, strings.TrimSpace(s))
	return s
}

func targetAllowed(target string) bool {
	if len(allowedTargets) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	for _, a := range allowedTargets {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if strings.Contains(a, "/") {
			_, ipnet, err := net.ParseCIDR(a)
			if err == nil && ipnet.Contains(ip) {
				return true
			}
			continue
		}
		// точное host:port или host (любой порт)
		if a == target || a == host {
			return true
		}
	}
	return false
}

// handlePing измеряет TCP RTT до целевого RDP-хоста (host:port).
// Браузерный клиент дёргает его периодически и показывает задержку рядом с FPS.
func handlePing(w http.ResponseWriter, r *http.Request) {
	target := normalizeTarget(r.URL.Query().Get("target"))
	if target == "" {
		http.Error(w, "missing target query parameter", http.StatusBadRequest)
		return
	}
	if !targetAllowed(target) {
		http.Error(w, "target not allowed", http.StatusForbidden)
		return
	}
	start := time.Now()
	conn, err := net.DialTimeout("tcp", target, 3*time.Second)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ms":-1,"error":%q}`, err.Error())
		return
	}
	conn.Close()
	ms := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ms":%d}`, ms)
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	target := normalizeTarget(r.URL.Query().Get("target"))
	if target == "" {
		http.Error(w, "missing target query parameter", http.StatusBadRequest)
		return
	}
	if !targetAllowed(target) {
		slog.Warn("target not allowed", "target", target, "remote", r.RemoteAddr)
		http.Error(w, "target not allowed", http.StatusForbidden)
		return
	}

	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("upgrade", "err", err)
		return
	}
	defer wsConn.Close()

	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second,
	}
	tcpConn, err := dialer.Dial("tcp", target)
	if err != nil {
		slog.Error("dial target", "target", target, "err", err)
		return
	}
	defer tcpConn.Close()

	start := time.Now()
	slog.Info("proxying", "target", target, "remote", r.RemoteAddr)

	// keepalive: пинг каждые 25 сек — держит NAT/релейные маппинги и
	// idle-таймауты (lighttpd server.max-read-idle=60с по умолчанию).
	// Браузер автоматически отвечает pong'ом.
	keepaliveStop := make(chan struct{})
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-keepaliveStop:
				return
			case <-t.C:
				wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := wsConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					slog.Warn("keepalive ping error", "target", target, "err", err)
					return
				}
			}
		}
	}()
	defer close(keepaliveStop)

	errc := make(chan error, 2)

	// WebSocket → TCP
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("PANIC ws->tcp", "target", target, "panic", r)
				errc <- fmt.Errorf("panic ws->tcp: %v", r)
			}
		}()
		for {
			mt, data, err := wsConn.ReadMessage()
			if err != nil {
				if ce, ok := err.(*websocket.CloseError); ok {
					// кто-то закрыл WS с кодом: 1006=transport, 1000/1001=clean
					slog.Warn("ws closed by peer", "target", target, "code", ce.Code, "reason", ce.Text)
				} else {
					slog.Warn("ws read error", "target", target, "err", err)
				}
				errc <- err
				return
			}
			if mt == websocket.BinaryMessage || mt == websocket.TextMessage {
				tcpConn.SetWriteDeadline(time.Now().Add(60 * time.Second))
				if _, err := tcpConn.Write(data); err != nil {
					slog.Warn("tcp write error", "target", target, "err", err)
					errc <- err
					return
				}
			}
		}
	}()

	// TCP → WebSocket
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("PANIC tcp->ws", "target", target, "panic", r)
				errc <- fmt.Errorf("panic tcp->ws: %v", r)
			}
		}()
		// 16КБ: меньше пиковые WS-фреймы → меньший бёрст на буфер клиента,
		// устойчивее через форвардеры с буферизацией (NDMS/Keenetic).
		buf := make([]byte, 16*1024)
		for {
			// Без read-дедлайна: тихий RDP-сервер (статичный экран) — норма,
			// kill после 60с тишины рвал живые сессии (i/o timeout в логе).
			// Мёртвых детектим иначе: браузер — WS-пингом каждые 25с,
			// молчавший TCP-пир — OS keepalive (dialer KeepAlive 30с).
			n, err := tcpConn.Read(buf)
			if n > 0 {
				// Запись в WS — с дедлайном (тут у нас есть контроль: браузер
				// должен отвечать, иначе сессия мёртва). Read — без.
				wsConn.SetWriteDeadline(time.Now().Add(60 * time.Second))
				if werr := wsConn.WriteMessage(websocket.BinaryMessage, buf[:n]); werr != nil {
					if ce, ok := werr.(*websocket.CloseError); ok {
						slog.Warn("ws write: closed by peer", "target", target, "code", ce.Code, "reason", ce.Text)
					} else {
						slog.Warn("ws write error", "target", target, "err", werr)
					}
					errc <- werr
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					slog.Warn("tcp read error", "target", target, "err", err)
				}
				if err == io.EOF {
					slog.Info("tcp closed by peer (EOF)", "target", target)
				}
				errc <- err
				return
			}
		}
	}()

	err = <-errc
	dur := time.Since(start).Round(time.Millisecond)
	if err != nil && err != io.EOF {
		slog.Warn("session end", "target", target, "duration", dur.String(), "err", err)
	} else {
		slog.Info("session end", "target", target, "duration", dur.String())
	}
}

func main() {
	// Логировать Info и выше (по умолчанию slog скрывает Info) — это ключевые
	// записи о причинах закрытия сессий, которые идут в /opt/var/log/entware/rdp.log.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	listen := flag.String("listen", "127.0.0.1:9099", "listen address")
	static := flag.String("static", "static", "directory to serve static files from")
	allow := flag.String("allow-target", "", "allowed RDP targets: comma-separated host:port, host, or CIDR subnet (empty = allow any)")
	flag.Parse()

	for _, a := range strings.Split(*allow, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			allowedTargets = append(allowedTargets, a)
		}
	}

	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/ping", handlePing)
	// Через reverse-proxy панели путь /rdp/ping приходит как /rdp/ping
	// (SingleHostReverseProxy не снимает префикс) — монтируем оба.
	http.HandleFunc("/rdp/ping", handlePing)
	http.Handle("/rdp/", noCacheHandler(gzipHandler(http.StripPrefix("/rdp", http.FileServer(http.Dir(*static))))))
	http.Handle("/", noCacheHandler(gzipHandler(http.FileServer(http.Dir(*static)))))

	log.Printf("Listening on %s (static: %s, allow-target: %q)", *listen, *static, *allow)
	if err := http.ListenAndServe(*listen, nil); err != nil {
		log.Fatal(err)
	}
}
