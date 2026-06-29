import React, {useState} from 'react';
import TOC from '@theme-original/TOC';
import type TOCType from '@theme/TOC';
import type {WrapperProps} from '@docusaurus/types';

type Props = WrapperProps<typeof TOCType>;

// Wraps the right-side table of contents with a collapse toggle so the
// secondary nav can be hidden, mirroring the hideable left sidebar.
export default function TOCWrapper(props: Props): JSX.Element {
  const [open, setOpen] = useState(true);

  // No headings on this page: render the original (which is empty) untouched.
  if (!props.toc || props.toc.length === 0) {
    return <TOC {...props} />;
  }

  return (
    <div className="tocCollapsible">
      <button
        type="button"
        className="tocCollapsibleToggle"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}>
        <span className={open ? 'tocChevron tocChevronOpen' : 'tocChevron'} aria-hidden="true" />
        On this page
      </button>
      {open && <TOC {...props} />}
    </div>
  );
}
