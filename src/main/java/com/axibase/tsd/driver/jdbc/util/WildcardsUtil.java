package com.axibase.tsd.driver.jdbc.util;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class WildcardsUtil {
	private static final char ONE_ANY_SYMBOL = '_';
	private static final char NONE_OR_MORE_SYMBOLS = '%';
	private static final char ATSD_ONE_ANY_SYMBOL = '?';
	private static final char ATSD_NONE_OR_MORE_SYMBOLS = '*';
	private static final char ESCAPE_CHAR = '\\';
	private static final int NOT_FOUND = -1;

	private static final Pattern SQL_WILDCARDS_PATTERN = Pattern.compile("(?<!\\\\)[%_]");
	private static final String[] ESCAPED_SQL_WILDCARDS = {"\\_", "\\%"};
	private static final String[] SQL_WILDCARDS = {"_", "%"};

	public static boolean hasWildcards(String text) {
		return text == null || SQL_WILDCARDS_PATTERN.matcher(text).find();
	}

	public static boolean hasAtsdWildcards(String text) {
		return text == null || hasWildcards(text, ATSD_ONE_ANY_SYMBOL, ATSD_NONE_OR_MORE_SYMBOLS);
	}

	private static boolean hasWildcards(String text, char oneSymbolWildcard, char manySymbolsWildcard) {
		return text == null || text.indexOf(oneSymbolWildcard) != NOT_FOUND || text.indexOf(manySymbolsWildcard) != NOT_FOUND;
	}

	public static boolean isRetrieveAllPattern(String text) {
		return text == null || (text.length() == 1 && text.charAt(0) == NONE_OR_MORE_SYMBOLS);
	}

	public static String wildcardToTableName(String pattern) {
		final int indexOfEscape = pattern.indexOf(ESCAPE_CHAR);
		if (indexOfEscape == NOT_FOUND) {
			return pattern;
		}
		return StringUtils.replaceEach(pattern, ESCAPED_SQL_WILDCARDS, SQL_WILDCARDS);
	}

	public static boolean wildcardMatch(String text, String pattern) {
		if (pattern == null) {
			return true;
		}
		if (text == null) {
			return false;
		}

		final String oneAnySymbolStr = String.valueOf(ONE_ANY_SYMBOL);
		final String noneOrMoreSymbolsStr = String.valueOf(NONE_OR_MORE_SYMBOLS);
		final int stringLength = text.length();
		final String[] wildcardTokens = splitOnTokens(pattern, oneAnySymbolStr, noneOrMoreSymbolsStr);
		boolean anyChars = false;
		int textIdx = 0;
		int wildcardTokensIdx = 0;
		final List<BacktrackContext> backtrack = new ArrayList<>();

		// loop around a backtrack stack to handle complex % matching
		do {
			final int backtrackListSize = backtrack.size();
			if (backtrackListSize > 0) {
				BacktrackContext context = backtrack.remove(backtrackListSize - 1);
				wildcardTokensIdx = context.tokenIndex;
				textIdx = context.charIndex;
				anyChars = true;
			}

			// loop whilst tokens and text left to process
			while (wildcardTokensIdx < wildcardTokens.length) {
				final String wildcardToken = wildcardTokens[wildcardTokensIdx];
				if (oneAnySymbolStr.equals(wildcardToken)) {
					// found one-symbol mask, hence move to next text char
					++textIdx;
					if (textIdx > stringLength) {
						break;
					}
					anyChars = false;
				} else if (noneOrMoreSymbolsStr.equals(wildcardToken)) {
					anyChars = true;
					if (wildcardTokensIdx == wildcardTokens.length - 1) {
						textIdx = stringLength;
					}
				} else {
					if (anyChars) {
						// any chars, hence try to locate text token
						textIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx);
						if (textIdx == NOT_FOUND) {
							break;
						}
						int repeatIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx  +1);
						if (repeatIdx >= 0) {
							backtrack.add(new BacktrackContext(wildcardTokensIdx, repeatIdx));
						}
					} else {
						// matching from current position
						if (!text.regionMatches(true, textIdx, wildcardToken, 0, wildcardToken.length())) {
							// couldn't match token
							break;
						}
					}

					// matched text token, move text index to end of matched token
					textIdx += wildcardToken.length();
					anyChars = false;
				}

				++wildcardTokensIdx;
			}

			// full match
			if (wildcardTokensIdx == wildcardTokens.length && textIdx == text.length()) {
				return true;
			}

		} while (!backtrack.isEmpty());

		return false;
	}

	/**
	 * Splits a string into a number of tokens.
	 * The text is split by '_' and '%'.
	 * Multiple '%' are collapsed into a single '%', patterns like "%_" will be transferred to "_%"
	 *
	 * @param text  the text to split
	 * @return the array of tokens, never null
	 */
	private static String[] splitOnTokens(String text, String oneAnySymbolStr, String noneOrMoreSymbolsStr) {
		if (!hasWildcards(text, ONE_ANY_SYMBOL, NONE_OR_MORE_SYMBOLS)) {
			return new String[] { text };
		}
		List<String> list = new ArrayList<>();
		StringBuilder buffer = new StringBuilder();

		boolean escapeMode = false;
		final int length = text.length();
		for (int i = 0; i < length; i++) {
			final char current = text.charAt(i);
			switch (current) {
				case ESCAPE_CHAR:
					if (escapeMode) {
						buffer.append(current);
					} else {
						escapeMode = true;
					}
					break;
				case ONE_ANY_SYMBOL:
					if (escapeMode) {
						buffer.append(current);
						escapeMode = false;
					} else {
						flushBuffer(buffer, list);
						if (!list.isEmpty() && noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
							list.set(list.size() - 1, oneAnySymbolStr);
							list.add(noneOrMoreSymbolsStr);
						} else {
							list.add(oneAnySymbolStr);
						}
					}
					break;
				case NONE_OR_MORE_SYMBOLS:
					if (escapeMode) {
						buffer.append(current);
						escapeMode = false;
					} else {
						flushBuffer(buffer, list);
						if (list.isEmpty() || !noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
							list.add(noneOrMoreSymbolsStr);
						}
					}
					break;
				default:
					if (escapeMode) {
						buffer.append(ESCAPE_CHAR);
						escapeMode = false;
					}
					buffer.append(current);
			}
		}
		if (escapeMode) {
			buffer.append(ESCAPE_CHAR);
		}
		if (buffer.length() != 0) {
			list.add(buffer.toString());
		}

		return list.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
	}

	public static String replaceSqlWildcardsWithAtsdUseEscaping(String text, boolean underscoreAsLiteral) {
		if (StringUtils.isEmpty(text)) {
			return text;
		}
		boolean modified = false;
		int newStrIndex = 0;
		final char[] chars = text.toCharArray();
		char[] newStrChars = chars;
		boolean escapeMode = false;
		for (final char symbol : chars) {
			switch (symbol) {
				case ESCAPE_CHAR:
					if (escapeMode) {
						newStrChars[newStrIndex++] = symbol;
					} else {
						escapeMode = true;
					}
					break;
				case ONE_ANY_SYMBOL:
					if (underscoreAsLiteral) {
						if (escapeMode) {
							newStrChars[newStrIndex++] = ESCAPE_CHAR;
							escapeMode = false;
						}
						newStrChars[newStrIndex++] = symbol;
					} else {
						if (escapeMode) {
							newStrChars[newStrIndex++] = symbol;
							escapeMode = false;
						} else {
							newStrChars[newStrIndex++] = ATSD_ONE_ANY_SYMBOL;
						}
						modified = true;
					}
					break;
				case NONE_OR_MORE_SYMBOLS:
					if (escapeMode) {
						newStrChars[newStrIndex++] = symbol;
						escapeMode = false;
					} else {
						newStrChars[newStrIndex++] = ATSD_NONE_OR_MORE_SYMBOLS;
					}
					modified = true;
					break;
				case ATSD_ONE_ANY_SYMBOL:
					if (escapeMode) {
						newStrChars[newStrIndex++] = ESCAPE_CHAR;
						escapeMode = false;
					}
					newStrChars = initNewStringCharArray(chars, newStrChars, newStrIndex);
					newStrChars[newStrIndex++] = ESCAPE_CHAR;
					newStrChars[newStrIndex++] = ATSD_ONE_ANY_SYMBOL;
					modified = true;
					break;
				case ATSD_NONE_OR_MORE_SYMBOLS:
					if (escapeMode) {
						newStrChars[newStrIndex++] = ESCAPE_CHAR;
						escapeMode = false;
					}
					newStrChars = initNewStringCharArray(chars, newStrChars, newStrIndex);
					newStrChars[newStrIndex++] = ESCAPE_CHAR;
					newStrChars[newStrIndex++] = ATSD_NONE_OR_MORE_SYMBOLS;
					modified = true;
					break;
				default:
					if (escapeMode) {
						newStrChars[newStrIndex++] = ESCAPE_CHAR;
						escapeMode = false;
					}
					newStrChars[newStrIndex++] = symbol;
			}
		}
		if (escapeMode) {
			newStrChars[newStrIndex++] = ESCAPE_CHAR;
		}

		return modified ? new String(newStrChars, 0, newStrIndex) : text;
	}

	private static char[] initNewStringCharArray(char[] old, char[] newArray, int index) {
		if (old == newArray) {
			newArray = new char[old.length * 2 - index];
			System.arraycopy(old, 0, newArray, 0, index);
		}
		return newArray;
	}

	private static void flushBuffer(StringBuilder buffer, List<String> list) {
		if (buffer.length() != 0) {
			list.add(buffer.toString());
			buffer.setLength(0);
		}
	}

	@AllArgsConstructor
	private static final class BacktrackContext {
		private final int tokenIndex;
		private final int charIndex;
	}

}
