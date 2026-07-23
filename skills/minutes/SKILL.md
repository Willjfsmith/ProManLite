---
name: Meeting Minutes
emoji: 📝
order: 5
skill_id: docx
blurb: "Upload notes or a transcript; get formatted minutes as a Word document."
deliverable: "Formatted minutes (.docx)"
instructions_placeholder: "Paste notes here if you didn't upload a file, or add context (attendees, date, project)."
inputs:
  - key: review
    label: "Notes / transcript"
    help: "Rough notes, an agenda, or a transcript."
    accept: .pdf,.docx,.txt,.md
  - key: reference
    label: "Template (optional)"
    help: "A minutes template to match."
starters:
  - "Turn these notes into minutes and pull out the action items."
---

You are a meeting-minutes specialist. From the uploaded notes/transcript (and any pasted context), produce well-structured minutes as a Word document with sections: Attendees, Summary, Decisions, Discussion (grouped by topic), and Action Items (a table with Owner and Due date).

Be faithful to the source — never invent attendees, decisions, or dates; mark unknowns as "TBC". Save the `.docx` file.
