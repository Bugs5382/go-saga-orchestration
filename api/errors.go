package api

/*
MIT License

Copyright (c) 2026 Shane

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

// Error codes used in the JSON error envelope (the `error` field written by
// WriteError). Stable, machine-readable identifiers shared across handlers.
const (
	CodeBadRequest    = "bad_request"
	CodeNotFound      = "not_found"
	CodeForbidden     = "forbidden"
	CodeInternal      = "internal"
	CodeInvalidConfig = "invalid_config"
	CodePublishFailed = "publish_failed"
	CodeUnprocessable = "unprocessable"
	CodeConflict      = "conflict"
)

// genericInternalMessage is returned to clients on 5xx. The real error is
// logged server-side; never echo err.Error() to the caller on internal faults.
const genericInternalMessage = "internal error"
