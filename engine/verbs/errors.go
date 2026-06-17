package verbs

/*
MIT License

Copyright (c) 2026 Bugs5382

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import "errors"

// ErrSagaPaused signals "the verb successfully suspended the saga; do
// not advance, do not fail." Coordinator recognises this and ACKs the
// RabbitMQ message without transitioning state to failed. The verb is
// responsible for persisting the pause state (wakeup_at /
// awaited_signal / awaited_event_*) via the store BEFORE returning
// this sentinel.
var ErrSagaPaused = errors.New("saga paused")

// ErrSagaCancelled is returned by the cancel verb for a self-cancel. Advance
// transitions the run to cancelled (terminal) and stops the loop.
var ErrSagaCancelled = errors.New("saga cancelled")
