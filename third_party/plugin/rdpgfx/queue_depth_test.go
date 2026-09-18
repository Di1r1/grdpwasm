package rdpgfx

import "testing"

// TestEffectiveDepth: queueDepth, который уходит серверу, — максимум из
// реальной длины очереди декодирования, пользовательской настройки
// «Quality» и внутреннего throttling'а ступора декодера.
func TestEffectiveDepth(t *testing.T) {
	g := &GfxHandler{}

	if got := g.effectiveDepth(3); got != 3 {
		t.Fatalf("no hints: got %d, want 3", got)
	}

	// Пользовательская настройка поднимает значение...
	g.SetQueueDepthHint(20)
	if got := g.effectiveDepth(3); got != 20 {
		t.Fatalf("user hint: got %d, want 20", got)
	}
	// ...но не занижает реальную очередь.
	if got := g.effectiveDepth(100); got != 100 {
		t.Fatalf("real queue must win: got %d, want 100", got)
	}

	// Внутренний stall-throttle учитывается отдельно и не затирает настройку.
	g.SetStallHint(50)
	if got := g.effectiveDepth(3); got != 50 {
		t.Fatalf("stall hint: got %d, want 50", got)
	}
	g.SetStallHint(0)
	if got := g.effectiveDepth(3); got != 20 {
		t.Fatalf("clearing stall must keep user hint: got %d, want 20", got)
	}

	// Сброс пользовательской настройки возвращает «как есть».
	g.SetQueueDepthHint(0)
	if got := g.effectiveDepth(7); got != 7 {
		t.Fatalf("hint cleared: got %d, want 7", got)
	}
}
