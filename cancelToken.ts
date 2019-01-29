export class CancelToken {
    private cancelled: boolean = false;
    isCancelled() {
        return this.cancelled;
    }
    cancel() {
        this.cancelled = true;
    }
}
