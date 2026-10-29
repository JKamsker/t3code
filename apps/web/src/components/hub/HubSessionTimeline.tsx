import type { LegendListRef } from "@legendapp/list/react";
import { EnvironmentId, type OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { createQuestionHistoryProjector } from "@t3tools/client-runtime/state/thread-requests";
import { useRef, useState } from "react";
import { useIncrementalTimelineEntries } from "~/hooks/useIncrementalTimelineEntries";
import { useTheme } from "~/hooks/useTheme";
import { MessagesTimeline, type MessagesTimelineHistoryControls } from "../chat/MessagesTimeline";
import type { TurnItemSource } from "../chat/timelineRowContext";
import { Button } from "../ui/button";

const EMPTY: readonly never[] = [];
const ATTACHMENTS = new Map<string, string>();
const ignore = () => {};
const HUB_ENVIRONMENT = EnvironmentId.make("hub-archive");

export function HubSessionTimeline({
  projection,
  turnItemSource,
  historyControls,
  onDownloadVisual,
}: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly turnItemSource: TurnItemSource;
  readonly historyControls: MessagesTimelineHistoryControls;
  readonly onDownloadVisual: (attachmentId: string, title: string) => void;
}) {
  const listRef = useRef<LegendListRef | null>(null);
  const { resolvedTheme } = useTheme();
  const [projectQuestions] = useState(() => createQuestionHistoryProjector());
  const threadKey = `hub-archive-${projection.thread.id}`;
  const entries = useIncrementalTimelineEntries(threadKey, {
    visibleTurnItems: projectQuestions(projection),
    optimisticMessages: EMPTY,
    attachmentUrlById: ATTACHMENTS,
    attempts: projection.attempts,
    nodes: projection.nodes,
    plans: projection.plans,
  });
  return (
    <div className="relative h-[min(65vh,42rem)] min-h-64 overflow-hidden rounded-lg border">
      <MessagesTimeline
        listRef={listRef}
        timelineEntries={entries}
        isWorking={false}
        latestRun={null}
        turnDiffSummaries={EMPTY}
        runs={projection.runs}
        routeThreadKey={threadKey}
        onOpenThread={ignore}
        supportsConversationRollback={false}
        onRevertToTurnCount={ignore}
        isRevertingCheckpoint={false}
        onImageExpand={ignore}
        turnItemSource={turnItemSource}
        historyControls={historyControls}
        renderHtmlRender={(row) => (
          <Button
            variant="outline"
            size="sm"
            onClick={() => onDownloadVisual(row.htmlRender.attachmentId, row.htmlRender.title)}
          >
            Download saved visual: {row.htmlRender.title}
          </Button>
        )}
        activeThreadEnvironmentId={HUB_ENVIRONMENT}
        markdownCwd={undefined}
        resolvedTheme={resolvedTheme}
        timestampFormat="locale"
        workspaceRoot={undefined}
        anchorMessageId={null}
        onAnchorReady={ignore}
        onAnchorSizeChanged={ignore}
        contentInsetEndAdjustment={0}
        emptyPlaceholderInset={0}
        liveFollowEnabled={false}
        onIsAtEndChange={ignore}
      />
    </div>
  );
}
